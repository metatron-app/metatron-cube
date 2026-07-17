/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.loading;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.ByteBufferUtils;
import io.druid.java.util.common.RangeProf;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper.RangeFetcher;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.QueryableIndex;
import io.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Budget + eviction for the direct off-heap memory held by range-served (header-first) segment columns.
 *
 * <p>A range segment's {@link QueryableIndex} is memoized for the segment's life (see
 * {@code SegmentLoaderLocalCacheManager.rangeSegment}), so its fetched column buffers ({@code allocateDirect}) stay
 * resident until the segment is dropped. A wide scan that touches thousands of range segments therefore accumulates
 * every touched segment's columns in direct memory with no cap — enough to OOM the container. This tracker bounds
 * that: it counts each materialized segment's fetched-column bytes and, when the total exceeds the budget, EVICTS
 * the coldest segments — "evict" = drop the memoized index reference so its direct buffers become unreachable and
 * are freed by their Cleaner, while the {@code LazySegment} stays in the timeline (still queryable, re-materializes
 * from deep storage on the next query).
 *
 * <p>Two triggers: (1) INLINE, on the column-fetch path, so a single wide query is bounded as its bytes grow —
 * this is what actually prevents the OOM, since the periodic sweep can't react within one multi-second query; and
 * (2) a periodic sweep that reconciles the running byte counter against the live set (self-healing any drift from
 * the rare eviction of a segment with an in-flight lazy fetch) and evicts down to the low watermark.
 *
 * <p>Eviction picks coldest-by-last-access; a segment being actively scanned has a just-updated access time, so it
 * is effectively never chosen. Even if it were, evict only drops the tracker's strong reference — an in-flight
 * query holds its own reference to the {@link QueryableIndex}, so the buffers live until that query releases them.
 */
public class RangeBufferTracker
{
  private static final Logger log = new Logger(RangeBufferTracker.class);

  private static final double HIGH_WM = 0.9;    // evict when resident > 90% of budget ...
  private static final double LOW_WM = 0.8;     // ... down to 80%
  private static final long SWEEP_MS = 120_000; // periodic reconcile + evict

  private final long budget;
  private final double keepWm;                  // below this fraction of budget, keep idle buffers (warm cache);
                                                // at or above, free them deterministically on query release
  private final RangeDiskCache diskCache;       // WARM (local-disk) tier under this direct-buffer tier; null = off
  private final int maxLiveIndexes;             // heap cap: max memoized indexes kept (skip re-open on warm); 0 = off
  private final AtomicLong resident = new AtomicLong();
  private final AtomicLong idleFrees = new AtomicLong();        // segments freed deterministically on query-release
  private final AtomicLong idleFreedBytes = new AtomicLong();
  private final Map<String, Materialization> live = new ConcurrentHashMap<>();
  private final Object evictLock = new Object();
  private final ScheduledExecutorService sweeper;

  public RangeBufferTracker(long budget)
  {
    this(budget, 0.5, null, 0);
  }

  public RangeBufferTracker(long budget, double keepRatio)
  {
    this(budget, keepRatio, null, 0);
  }

  public RangeBufferTracker(long budget, double keepRatio, RangeDiskCache diskCache, int maxLiveIndexes)
  {
    this.budget = budget;
    this.keepWm = Math.max(0.0, Math.min(1.0, keepRatio));
    this.diskCache = diskCache;
    this.maxLiveIndexes = maxLiveIndexes;
    this.sweeper = Execs.scheduledSingleThreaded("range-residency-%d");
    this.sweeper.scheduleWithFixedDelay(this::sweep, SWEEP_MS, SWEEP_MS, TimeUnit.MILLISECONDS);
    log.info("RangeBufferTracker started: budget=%,d high=%.2f low=%.2f keep=%.2f sweep=%dms diskCache=%s maxLiveIndexes=%d",
             budget, HIGH_WM, LOW_WM, keepWm, SWEEP_MS, diskCache != null, maxLiveIndexes);
  }

  /**
   * Wrap a range segment's index build into a memoizing, accounted, evictable loader. {@code build} materializes
   * the index from the fetcher it is handed; {@code rawFetcher} supplies the underlying (uncounted) deep-storage
   * fetcher. The returned supplier is what a {@code LazySegment} loads from.
   */
  public Supplier<QueryableIndex> track(DataSegment segment, Function<RangeFetcher, QueryableIndex> build, Supplier<RangeFetcher> rawFetcher)
  {
    final Materialization mat = new Materialization(segment, build, rawFetcher);
    return mat::get;
  }

  /**
   * A range segment's last in-flight query reference was released (see {@code ReferenceCountingSegment} wiring in
   * {@code ServerManager}), so freeing its column buffers now can't race a reader. Adaptive: under memory pressure
   * (resident at/above {@code keepWm} of budget) free them DETERMINISTICALLY (don't wait for the Cleaner — that GC
   * lag is what let concurrent-fetch bursts overshoot MaxDirectMemory); with headroom, keep them memoized as a warm
   * cache so the next query needn't re-fetch from deep storage.
   */
  public void onIdle(String segmentId)
  {
    // Free direct buffers deterministically only under memory pressure (resident at/above keepWm of budget); with
    // headroom, KEEP the memoized index as a warm cache so the next query skips both the fetch AND the
    // DirectoryReader.open. A disk-served segment adds ~0 to `resident` (its buffers are mmap page cache, not direct),
    // so it stays memoized here and is instead bounded by the maxLiveIndexes count cap — which is what lets warm
    // repeats over a disk-cached corpus avoid re-opening every segment.
    if (resident.get() < keepWm * budget) {
      return;
    }
    final Materialization m = live.get(segmentId);
    if (m != null) {
      m.release();
    }
  }

  public long residentBytes()
  {
    return resident.get();
  }

  public long budgetBytes()
  {
    return budget;
  }

  public void stop()
  {
    sweeper.shutdownNow();
  }

  /** Inline back-pressure: called after each column fetch grows the resident total. */
  private void admit()
  {
    if (resident.get() > HIGH_WM * budget) {
      evictColdestUntil((long) (LOW_WM * budget));
    }
  }

  private void sweep()
  {
    try {
      // reconcile the running counter to the true live sum (corrects drift from evicting an in-flight segment)
      long sum = 0;
      for (Materialization m : live.values()) {
        sum += m.bytes.get();
      }
      resident.set(sum);
      final long frees = idleFrees.getAndSet(0);
      if (frees > 0) {
        log.info("[range-residency] freed %d idle segment(s) (~%,d bytes) deterministically on release; resident ~%,d / %,d",
                 frees, idleFreedBytes.getAndSet(0), sum, budget);
      }
      if (sum > HIGH_WM * budget) {
        evictColdestUntil((long) (LOW_WM * budget));
      }
      if (maxLiveIndexes > 0 && live.size() > maxLiveIndexes) {
        evictColdestByCount((int) (LOW_WM * maxLiveIndexes));
      }
    }
    catch (Throwable t) {
      log.warn(t, "[range-residency] sweep failed");
    }
  }

  /** Heap cap: dematerialize the coldest memoized indexes until at most {@code target} remain. Keeps warm segments
   *  (recent access) memoized so their queries skip re-open; only the cold tail is dropped. */
  private void evictColdestByCount(int target)
  {
    synchronized (evictLock) {
      if (live.size() <= target) {
        return;
      }
      final List<Materialization> coldest = new ArrayList<>(live.values());
      coldest.sort(Comparator.comparingLong(m -> m.lastAccess));   // coldest (oldest access) first
      int evicted = 0;
      for (Materialization m : coldest) {
        if (live.size() <= target) {
          break;
        }
        if (m.evict()) {
          evicted++;
        }
      }
      if (evicted > 0) {
        log.info("[range-residency] dropped %d cold index(es) over the %d cap; live ~%d", evicted, maxLiveIndexes, live.size());
      }
    }
  }

  private void evictColdestUntil(long target)
  {
    synchronized (evictLock) {
      if (resident.get() <= target) {
        return;
      }
      final List<Materialization> coldest = new ArrayList<>(live.values());
      coldest.sort(Comparator.comparingLong(m -> m.lastAccess));   // coldest (oldest access) first
      int evicted = 0;
      for (Materialization m : coldest) {
        if (resident.get() <= target) {
          break;
        }
        if (m.evict()) {
          evicted++;
        }
      }
      if (evicted > 0) {
        log.info("[range-residency] evicted %d segment(s); direct ~%,d / %,d bytes", evicted, resident.get(), budget);
      }
    }
  }

  /** One range segment's memoized index + its accounted, evictable residency. */
  private final class Materialization
  {
    private final String id;
    private final String cacheKey;   // getStorageDir(segment): the per-segment disk-cache path (stable across evict)
    private final Function<RangeFetcher, QueryableIndex> build;
    private final Supplier<RangeFetcher> rawFetcher;
    private final AtomicLong bytes = new AtomicLong();
    private final List<java.nio.ByteBuffer> buffers = Collections.synchronizedList(new ArrayList<>());  // for explicit free
    private volatile QueryableIndex index;   // strong ref == materialized (buffers resident)
    private volatile long lastAccess;

    private Materialization(DataSegment segment, Function<RangeFetcher, QueryableIndex> build, Supplier<RangeFetcher> rawFetcher)
    {
      this.id = segment.getIdentifier();
      this.cacheKey = DataSegmentPusherUtil.getStorageDir(segment);
      this.build = build;
      this.rawFetcher = rawFetcher;
    }

    private QueryableIndex get()
    {
      lastAccess = System.currentTimeMillis();
      QueryableIndex idx;
      boolean materialized = false;
      synchronized (this) {
        if (index == null) {
          index = build.apply(counting());   // columns are then fetched lazily through the counting fetcher
          live.put(id, this);
          materialized = true;
        }
        idx = index;
      }
      // enforce the heap cap OUTSIDE this Materialization's lock (evict touches other Materializations' locks); only
      // on a fresh materialize, and never picks this one — its just-set lastAccess makes it the hottest.
      if (materialized && maxLiveIndexes > 0 && live.size() > maxLiveIndexes) {
        evictColdestByCount((int) (LOW_WM * maxLiveIndexes));
      }
      return idx;   // returned to the query; safe to evict `index` afterwards (the query keeps this strong ref)
    }

    private synchronized boolean evict()
    {
      if (index == null) {
        return false;
      }
      index = null;
      resident.addAndGet(-bytes.getAndSet(0));
      // MUST drop the retained buffer refs too, else this Materialization (reachable via the LazySegment loader)
      // keeps the direct ByteBuffers strongly reachable forever -> their Cleaner never runs -> the off-heap memory
      // is never reclaimed even under pressure (allocations then OOM). Clear (not free): an in-flight query still
      // holds the QueryableIndex, so the Cleaner frees only once it's truly unreachable — safe here (unlike the
      // release() explicit free, which is gated on zero query refs).
      buffers.clear();
      live.remove(id);
      return true;
    }

    /**
     * Deterministic free: drop the memoized index AND explicitly free its column buffers now (via
     * {@link ByteBufferUtils#free}) instead of leaving them to the Cleaner. Only safe when no query holds the index —
     * callers gate on the segment's query ref-count being zero (see {@link RangeBufferTracker#onIdle}). Synchronized
     * against {@link #get()} so a re-materializing query can't observe half-freed state.
     */
    private synchronized void release()
    {
      if (index == null) {
        return;
      }
      index = null;
      final long freed = bytes.getAndSet(0);
      resident.addAndGet(-freed);
      live.remove(id);
      synchronized (buffers) {
        for (java.nio.ByteBuffer b : buffers) {
          ByteBufferUtils.free(b);
        }
        buffers.clear();
      }
      idleFrees.incrementAndGet();
      idleFreedBytes.addAndGet(freed);
    }

    private RangeFetcher counting()
    {
      final RangeFetcher delegate = rawFetcher.get();
      return (fileNum, offset, length) -> {
        // WARM tier: a disk-cache hit is an mmap'd (page-cache) buffer — serve it WITHOUT counting against the
        // direct budget or retaining it for explicit free (the OS reclaims page cache; the Cleaner unmaps it when
        // the query drops the index). This is why a re-materialize after eviction costs ~0 direct memory.
        if (diskCache != null) {
          final long tDisk = System.nanoTime();
          final java.nio.ByteBuffer cached = diskCache.get(cacheKey, fileNum, offset, length);
          if (cached != null) {
            RangeProf.fetchDiskNanos.addAndGet(System.nanoTime() - tDisk);
            RangeProf.fetchDiskBytes.addAndGet(cached.remaining());
            RangeProf.fetchDiskCount.incrementAndGet();
            return cached;
          }
        }
        // miss: fetch from deep storage, write through to the WARM tier, and account it in the HOT (direct) tier
        final long tS3 = System.nanoTime();
        final java.nio.ByteBuffer buf = delegate.fetch(fileNum, offset, length);
        final long s3Nanos = System.nanoTime() - tS3;
        if (buf != null) {
          if (diskCache != null) {
            RangeProf.fetchS3Nanos.addAndGet(s3Nanos);
            RangeProf.fetchS3Bytes.addAndGet(buf.capacity());
            RangeProf.fetchS3Count.incrementAndGet();
            diskCache.put(cacheKey, fileNum, offset, buf);   // does not consume buf (writes a duplicate)
          }
          buffers.add(buf);   // retained so release() can free it deterministically
          final long n = buf.capacity();
          bytes.addAndGet(n);
          resident.addAndGet(n);
          admit();
        }
        return buf;
      };
    }
  }
}
