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

import io.druid.java.util.common.logger.Logger;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WARM (local-disk) tier under {@link RangeBufferTracker}'s direct-buffer (HOT) tier. A range-served column fetch
 * is a deterministic {@code (segment, fileNum, offset, length)} byte-range of the deep-storage smoosh, so it is a
 * content-stable cache key. On the first fetch we write the bytes through to a per-segment sparse file at the SAME
 * offset as the smoosh; a later fetch of the same range (typically after its HOT direct buffer was evicted) is
 * served by {@code mmap}-ing that region — page cache, so it does NOT count against {@code -XX:MaxDirectMemorySize}
 * — instead of re-fetching from deep storage. Only fetched (queried) columns are ever written, so the fat unqueried
 * columns (e.g. a wide {@code raw}) are never localized, unlike {@code loadMode=download}.
 *
 * <p>Managed granularity is PER SEGMENT (one directory, one size, one last-access), the same order of magnitude as
 * the HOT tier's per-segment Materializations — the sub-segment detail is pushed to the OS: sparse files hold only
 * the fetched ranges (holes cost no disk) and the page cache holds only touched pages. Eviction is whole-segment
 * LRU: {@code unlink} the segment's directory (Linux keeps the inode alive for any query still mmap-reading it).
 *
 * <p>v1 scope: the root is cleared on boot (no cross-restart reuse) and writes are not fsync'd, so a crash can
 * leave a partially-written range — harmless here because a range is only recorded (and thus servable) AFTER its
 * write returns, and nothing is trusted across a restart. Cross-restart persistence + fsync'd completed-range logs
 * are a deliberate follow-up.
 */
public class RangeDiskCache
{
  private static final Logger log = new Logger(RangeDiskCache.class);

  private static final double LOW_WM = 0.9;   // evict down to this fraction of maxBytes when over budget

  private final File root;
  private final long maxBytes;
  private final Map<String, Seg> segments = new ConcurrentHashMap<>();
  private final AtomicLong resident = new AtomicLong();
  private final Object evictLock = new Object();

  public RangeDiskCache(File root, long maxBytes)
  {
    this.root = root;
    this.maxBytes = maxBytes;
    if (!root.mkdirs() && !root.isDirectory()) {
      throw new IllegalStateException("range-disk cache root not usable: " + root);
    }
    try {
      // clean slate: v1 does not trust on-disk ranges across a restart (no fsync'd completed-range log yet). Clean
      // the CONTENTS, not the dir itself — the path is typically a mount point (emptyDir), which cannot be removed.
      FileUtils.cleanDirectory(root);
    }
    catch (IOException e) {
      log.warn(e, "[range-disk] could not clear cache root[%s] on boot", root);
    }
    log.info("RangeDiskCache started: root=%s maxBytes=%,d", root, maxBytes);
  }

  /**
   * Serve a cached range as an mmap'd (page-cache) buffer, or {@code null} on a miss. The returned buffer is
   * independent (position 0, limit=length) and backed by the OS page cache — not the direct-memory budget.
   */
  public ByteBuffer get(String segKey, int fileNum, long offset, int length)
  {
    final Seg seg = segments.get(segKey);
    if (seg == null) {
      return null;
    }
    final Map<Long, Integer> fileRanges = seg.ranges.get(fileNum);
    final Integer cachedLen = fileRanges == null ? null : fileRanges.get(offset);
    if (cachedLen == null || cachedLen != length) {
      return null;   // absent, or a different length at this offset -> treat as miss
    }
    try {
      seg.lastAccess = System.currentTimeMillis();
      return seg.slice(fileNum, offset, length);   // sliced from a per-file whole-map (one mmap per file, not per get)
    }
    catch (IOException e) {
      log.warn(e, "[range-disk] map failed seg[%s] file[%d] off[%d] len[%d] -> miss", segKey, fileNum, offset, length);
      return null;
    }
  }

  /**
   * Write-through a freshly deep-storage-fetched range. Idempotent: a range already present is a no-op. {@code data}
   * is not consumed (a read-only duplicate is written). Records the range only AFTER the bytes are on disk, so a
   * concurrent {@link #get} never observes a not-yet-written range.
   */
  public void put(String segKey, int fileNum, long offset, ByteBuffer data)
  {
    final int length = data.remaining();
    if (length <= 0) {
      return;
    }
    final Seg seg = segments.computeIfAbsent(segKey, k -> new Seg(k, new File(root, k)));
    final Map<Long, Integer> fileRanges = seg.ranges.computeIfAbsent(fileNum, k -> new ConcurrentHashMap<>());
    if (fileRanges.containsKey(offset)) {
      return;   // already cached (fast path, no write)
    }
    try {
      final FileChannel ch = seg.channel(fileNum);
      final ByteBuffer dup = data.duplicate();   // independent position/limit; same bytes
      int written = 0;
      while (dup.hasRemaining()) {
        written += ch.write(dup, offset + written);   // positional write -> sparse extend, no shared cursor
      }
      if (fileRanges.putIfAbsent(offset, length) == null) {   // record only after the bytes are durable in the file
        seg.bytes.addAndGet(length);
        if (resident.addAndGet(length) > maxBytes) {
          evictColdestUntil((long) (LOW_WM * maxBytes), segKey);
        }
      }
    }
    catch (IOException e) {
      log.warn(e, "[range-disk] write failed seg[%s] file[%d] off[%d] len[%d] -> not cached",
               segKey, fileNum, offset, length);
    }
  }

  public long residentBytes()
  {
    return resident.get();
  }

  private void evictColdestUntil(long target, String keepSegKey)
  {
    synchronized (evictLock) {
      if (resident.get() <= target) {
        return;
      }
      final List<Seg> coldest = new ArrayList<>(segments.values());
      coldest.sort(Comparator.comparingLong(s -> s.lastAccess));   // oldest access first
      int evicted = 0;
      for (Seg s : coldest) {
        if (resident.get() <= target) {
          break;
        }
        if (s.key().equals(keepSegKey)) {
          continue;   // never evict the segment we are currently populating
        }
        if (drop(s)) {
          evicted++;
        }
      }
      if (evicted > 0) {
        log.info("[range-disk] evicted %d segment(s); disk ~%,d / %,d bytes", evicted, resident.get(), maxBytes);
      }
    }
  }

  private boolean drop(Seg seg)
  {
    if (segments.remove(seg.key()) == null) {
      return false;
    }
    resident.addAndGet(-seg.bytes.getAndSet(0));
    seg.close();
    try {
      FileUtils.deleteDirectory(seg.dir);   // unlink; any in-flight mmap keeps the inode alive on Linux
    }
    catch (IOException e) {
      log.warn(e, "[range-disk] could not delete evicted seg dir[%s]", seg.dir);
    }
    return true;
  }

  /** Per-segment state: one sparse cache file per smoosh chunk (fileNum), plus the recorded (offset -> length) ranges. */
  private static final class Seg
  {
    private final String key;   // the segKey (getStorageDir) — a multi-level relative path, not dir.getName()
    private final File dir;
    private final Map<Integer, FileChannel> channels = new ConcurrentHashMap<>();
    private final Map<Integer, Map<Long, Integer>> ranges = new ConcurrentHashMap<>();
    private final Map<Integer, MappedByteBuffer> wholeMaps = new ConcurrentHashMap<>();   // one mmap per chunk file
    private final AtomicLong bytes = new AtomicLong();
    private volatile long lastAccess = System.currentTimeMillis();

    private Seg(String key, File dir)
    {
      this.key = key;
      this.dir = dir;
      dir.mkdirs();
    }

    private String key()
    {
      return key;
    }

    /**
     * A read-only view of {@code [offset, offset+length)} in chunk {@code fileNum}, sliced from a cached whole-file
     * mmap so a query's thousands of range reads cost ONE mmap per file, not one per range. The whole-map is
     * (re)created lazily and re-created if the file has grown past it (a range is only ever recorded after its write
     * extends the file, so a recorded range is always within the current file size). Regions past 2GB — beyond a
     * single MappedByteBuffer — fall back to a per-call map (smoosh chunks are normally well under 2GB).
     */
    private ByteBuffer slice(int fileNum, long offset, int length) throws IOException
    {
      final long end = offset + length;
      if (end > Integer.MAX_VALUE) {
        return channel(fileNum).map(FileChannel.MapMode.READ_ONLY, offset, length);
      }
      MappedByteBuffer whole = wholeMaps.get(fileNum);
      if (whole == null || whole.capacity() < end) {
        synchronized (this) {
          whole = wholeMaps.get(fileNum);
          if (whole == null || whole.capacity() < end) {
            final FileChannel ch = channel(fileNum);
            whole = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size());   // remap to current size covers the range
            wholeMaps.put(fileNum, whole);   // a prior map stays alive via any in-flight slices' parent reference
          }
        }
      }
      final ByteBuffer dup = whole.duplicate();
      dup.position((int) offset).limit((int) end);
      return dup.slice();
    }

    private FileChannel channel(int fileNum) throws IOException
    {
      FileChannel ch = channels.get(fileNum);
      if (ch != null) {
        return ch;
      }
      synchronized (channels) {
        ch = channels.get(fileNum);
        if (ch == null) {
          final File f = new File(dir, fileNum + ".cache");
          ch = FileChannel.open(f.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                                StandardOpenOption.WRITE);
          channels.put(fileNum, ch);
        }
        return ch;
      }
    }

    private void close()
    {
      wholeMaps.clear();   // drop map refs so the Cleaner can unmap once in-flight slices release them
      synchronized (channels) {
        for (FileChannel ch : channels.values()) {
          try {
            ch.close();
          }
          catch (IOException ignored) {
            // best-effort on evict
          }
        }
        channels.clear();
      }
    }
  }
}
