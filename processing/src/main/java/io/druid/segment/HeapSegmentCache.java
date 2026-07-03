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

package io.druid.segment;

import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A bounded, LRU heap-resident segment cache. Segments are loaded PURELY into heap ByteBuffers
 * ({@link SmooshedFileMapper#loadHeap}) — no mmap/tmpfs. Total heap footprint is capped at {@code maxBytes};
 * when a new load would exceed it, least-recently-used entries with no live references are evicted (closed →
 * their heap buffers become garbage). Entries currently held via {@link Handle} are pinned (never evicted).
 *
 * This is the residency manager the historical would need for an on-heap cold-segment pool (there is no OS page
 * cache to auto-evict heap buffers, unlike mmap). A soft cap: if everything is pinned it over-commits rather
 * than fail a query.
 */
public final class HeapSegmentCache implements Closeable
{
  private static final Logger log = new Logger(HeapSegmentCache.class);

  private final IndexIO indexIO;
  private final long maxBytes;
  // access-order LRU: iteration yields least-recently-used first
  private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
  private long usedBytes;

  public HeapSegmentCache(IndexIO indexIO, long maxBytes)
  {
    this.indexIO = indexIO;
    this.maxBytes = maxBytes;
  }

  /**
   * Pin the segment (loading it into heap on a miss, evicting LRU unpinned entries to make room) and return a
   * {@link Handle}. The caller MUST close the handle when done so the entry can later be evicted.
   */
  public synchronized Handle acquire(String id, File dir) throws IOException
  {
    Entry entry = entries.get(id);
    if (entry == null) {
      final long bytes = heapFootprint(dir);
      evictToFit(bytes);
      final QueryableIndex index = indexIO.loadIndex(dir, false, SmooshedFileMapper.loadHeap(dir));
      entry = new Entry(index, bytes);
      entries.put(id, entry);
      usedBytes += bytes;
      log.debug("loaded [%s] into heap (%,d bytes); used=%,d/%,d", id, bytes, usedBytes, maxBytes);
    }
    entry.refs++;
    return new Handle(id, entry.index);
  }

  /**
   * Like {@link #acquire(String, File)} but the segment's files are produced on demand by {@code fetcher} (e.g.
   * pulled from deep storage into a temp dir). After the segment is read fully into heap the fetched files are
   * deleted — heap holds the bytes, so residency stays zero-disk. This is the cold-segment lazy-fetch entry.
   */
  public synchronized Handle acquire(String id, Fetcher fetcher) throws IOException
  {
    Entry entry = entries.get(id);
    if (entry == null) {
      final File dir = fetcher.fetch();
      try {
        final long bytes = heapFootprint(dir);
        evictToFit(bytes);
        final QueryableIndex index = indexIO.loadIndex(dir, false, SmooshedFileMapper.loadHeap(dir));
        entry = new Entry(index, bytes);
        entries.put(id, entry);
        usedBytes += bytes;
        log.debug("fetched+loaded [%s] into heap (%,d bytes); used=%,d/%,d", id, bytes, usedBytes, maxBytes);
      }
      finally {
        deleteQuietly(dir);   // heap now holds the bytes (eager loadHeap); drop the fetched files
      }
    }
    entry.refs++;
    return new Handle(id, entry.index);
  }

  /**
   * Load-on-miss and return the heap-resident index WITHOUT pinning — for a {@link LazySegment}'s materializer,
   * which is called per query access. Marks the entry most-recently-used so a fresh load is not the immediate
   * eviction victim. (Eviction of a segment mid-query is prevented properly by tying a pin to the query
   * ref-count — the deferred concurrency-hardening step; single-access flows are safe via LRU recency.)
   */
  public synchronized QueryableIndex getOrLoad(String id, Fetcher fetcher) throws IOException
  {
    Entry entry = entries.get(id);
    if (entry == null) {
      try (Handle h = acquire(id, fetcher)) {   // acquire loads+pins; close() unpins, leaving it MRU + resident
        return h.index();
      }
    }
    entries.get(id);   // touch for LRU recency (access-order)
    return entry.index;
  }

  /**
   * Fully in-memory load-on-miss: the fetcher returns the segment's {@code index.zip} BYTES (e.g. a straight S3
   * GET), which are unzipped and heap-loaded with NO temp file at all (see
   * {@link SmooshedFileMapper#loadHeapFromZip}). No pin — for a {@link LazySegment} materializer.
   */
  public synchronized QueryableIndex getOrLoad(String id, BytesFetcher fetcher) throws IOException
  {
    Entry entry = entries.get(id);
    if (entry == null) {
      final byte[] zip = fetcher.fetch();
      final long bytes = zip.length;   // ~uncompressed footprint (segment zips are stored uncompressed)
      evictToFit(bytes);
      final QueryableIndex index = indexIO.loadIndex(null, false, SmooshedFileMapper.loadHeapFromZip(zip));
      entry = new Entry(index, bytes);
      entries.put(id, entry);
      usedBytes += bytes;
      log.debug("loaded [%s] into heap from bytes (%,d); used=%,d/%,d", id, bytes, usedBytes, maxBytes);
      return index;
    }
    return entry.index;
  }

  private static void deleteQuietly(File dir)
  {
    final File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        f.delete();
      }
    }
    dir.delete();
  }

  private void evictToFit(long need)
  {
    if (usedBytes + need <= maxBytes) {
      return;
    }
    final Iterator<Map.Entry<String, Entry>> it = entries.entrySet().iterator();
    while (it.hasNext() && usedBytes + need > maxBytes) {
      final Map.Entry<String, Entry> e = it.next();
      if (e.getValue().refs > 0) {
        continue;   // pinned by a live query — cannot evict
      }
      CloseQuietly.close(e.getValue().index);
      usedBytes -= e.getValue().bytes;
      it.remove();
      log.debug("evicted [%s] (%,d bytes); used=%,d/%,d", e.getKey(), e.getValue().bytes, usedBytes, maxBytes);
    }
  }

  private synchronized void release(String id)
  {
    final Entry entry = entries.get(id);
    if (entry != null && entry.refs > 0) {
      entry.refs--;
    }
  }

  // heap footprint = bytes loadHeap wraps = the smoosh chunk files (NNNNN.smoosh); meta/version are trivial
  private static long heapFootprint(File dir)
  {
    long total = 0;
    final File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isFile() && f.getName().matches("\\d+\\.smoosh")) {
          total += f.length();
        }
      }
    }
    return total;
  }

  public synchronized long usedBytes()
  {
    return usedBytes;
  }

  public synchronized int size()
  {
    return entries.size();
  }

  public synchronized boolean isCached(String id)
  {
    return entries.containsKey(id);
  }

  @Override
  public synchronized void close()
  {
    for (Entry entry : entries.values()) {
      CloseQuietly.close(entry.index);
    }
    entries.clear();
    usedBytes = 0;
  }

  /** Produces the segment's files locally on a cache miss (e.g. pulls index from deep storage into a temp dir). */
  public interface Fetcher
  {
    File fetch() throws IOException;
  }

  /** Produces the segment's index.zip BYTES on a cache miss (e.g. an S3 GET) — for the no-temp-file heap path. */
  public interface BytesFetcher
  {
    byte[] fetch() throws IOException;
  }

  private static final class Entry
  {
    private final QueryableIndex index;
    private final long bytes;
    private int refs;

    private Entry(QueryableIndex index, long bytes)
    {
      this.index = index;
      this.bytes = bytes;
    }
  }

  /** A pinned reference to a cached segment; close() releases the pin so the entry becomes evictable. */
  public final class Handle implements Closeable
  {
    private final String id;
    private final QueryableIndex index;
    private boolean closed;

    private Handle(String id, QueryableIndex index)
    {
      this.id = id;
      this.index = index;
    }

    public QueryableIndex index()
    {
      return index;
    }

    @Override
    public void close()
    {
      if (!closed) {
        closed = true;
        release(id);
      }
    }
  }
}
