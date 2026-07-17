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

package io.druid.java.util.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lives in java-util so all three measurement points can reach it: the range-fetch is in
 * {@code SmooshedFileMapper} (java-util), the lucene open is in {@code Lucenes} (lucene-common), the lucene search is
 * in {@code LuceneIndexingSpec} (lucene-common), and the per-query snapshot log is in {@code ServerManager} (server) —
 * none of which share a module below java-util.
 *
 * <p>Splits a range-served lucene query's cost into FETCH (bytes pulled from deep storage), OPEN
 * ({@code DirectoryReader.open} parsing each segment's index structure) and SEARCH. The three run sequentially on
 * each processing thread per segment, so the SUM across threads is thread-time and the RATIO fetch:open:search is
 * concurrency-invariant — that ratio tells us whether a wide lucene scan is fetch-byte-bound (partial-fetch would cut
 * wall-clock) or open/search-bound (partial-fetch is a memory win only). Measurement scaffold; not wired to configs.
 */
public final class RangeProf
{
  public static final AtomicLong fetchNanos = new AtomicLong();
  public static final AtomicLong fetchBytes = new AtomicLong();
  public static final AtomicLong fetchCount = new AtomicLong();

  // FETCH split by tier (populated only when the range disk-cache is on): a fetch is served either from the local
  // disk cache (a HIT — mmap, page cache) or from deep storage (a MISS — S3). fetchBytes/Count above are the total
  // (tier-agnostic, since they wrap the outer fetcher call); disk + s3 below break that down, and the disk-vs-s3
  // NANOS is the real signal — same bytes, but disk is far faster than S3.
  public static final AtomicLong fetchDiskNanos = new AtomicLong();
  public static final AtomicLong fetchDiskBytes = new AtomicLong();
  public static final AtomicLong fetchDiskCount = new AtomicLong();
  public static final AtomicLong fetchS3Nanos = new AtomicLong();
  public static final AtomicLong fetchS3Bytes = new AtomicLong();
  public static final AtomicLong fetchS3Count = new AtomicLong();

  public static final AtomicLong openNanos = new AtomicLong();
  public static final AtomicLong openCount = new AtomicLong();

  public static final AtomicLong searchNanos = new AtomicLong();
  public static final AtomicLong searchCount = new AtomicLong();

  // Buffer-fill probe (RangeFetchIndexInput): is RANGE_BUFFER_SIZE (64K) the right read-ahead window, and would a
  // prefetch() override pay off? Every readInternal is one range-GET. We split them into:
  //   - REFILL: a BufferedIndexInput buffer top-up (len <= bufferSize). Its contiguity vs the previous GET on the same
  //     clone is the over/under signal: SEQ (starts exactly where the last ended) = sequential read chopped into 64K
  //     GETs -> a BIGGER buffer would merge them (under-fetch); SEEK (non-contiguous) = the last 64K was likely
  //     abandoned mid-way after a few needed bytes -> over-fetch. bufRefillBytes/Count give avg fill size.
  //   - DIRECT: a bulk read bigger than the buffer (len > bufferSize) that bypasses buffering entirely -> the 64K knob
  //     is irrelevant for these; a large postings scan shows up here.
  //   - PREFETCH: Lucene's prefetch(offset,len) hint calls (term-dict/postings). Count only (the override stays a
  //     no-op); the per-event offsets are logged separately under -Ddruid.lucene.rangeProbeLog=true for correlation.
  public static final AtomicLong bufRefillCount = new AtomicLong();
  public static final AtomicLong bufRefillBytes = new AtomicLong();
  public static final AtomicLong bufSeqRefills = new AtomicLong();
  public static final AtomicLong bufSeekRefills = new AtomicLong();
  public static final AtomicLong bufDirectCount = new AtomicLong();
  public static final AtomicLong bufDirectBytes = new AtomicLong();
  public static final AtomicLong prefetchCount = new AtomicLong();

  private RangeProf() {}

  /** Snapshot the current counters, resetting them to zero, and render a one-line summary. */
  public static String snapshotAndReset()
  {
    final long fN = fetchNanos.getAndSet(0), fB = fetchBytes.getAndSet(0), fC = fetchCount.getAndSet(0);
    final long dN = fetchDiskNanos.getAndSet(0), dB = fetchDiskBytes.getAndSet(0), dC = fetchDiskCount.getAndSet(0);
    final long s3N = fetchS3Nanos.getAndSet(0), s3B = fetchS3Bytes.getAndSet(0), s3C = fetchS3Count.getAndSet(0);
    final long oN = openNanos.getAndSet(0), oC = openCount.getAndSet(0);
    final long sN = searchNanos.getAndSet(0), sC = searchCount.getAndSet(0);
    final double total = Math.max(1, fN + oN + sN);
    String line = StringUtils.safeFormat(
        "fetch %,dms (%,dMB, %d gets, %.0f%%) | open %,dms (%d, %.0f%%) | search %,dms (%d, %.0f%%)",
        fN / 1_000_000, fB / (1024 * 1024), fC, 100 * fN / total,
        oN / 1_000_000, oC, 100 * oN / total,
        sN / 1_000_000, sC, 100 * sN / total
    );
    if (dC + s3C > 0) {   // disk cache on: show the hit/miss split (disk vs s3), the nanos being the real signal
      line += StringUtils.safeFormat(
          "  [disk %,dms %,dMB %d gets | s3 %,dms %,dMB %d gets]",
          dN / 1_000_000, dB / (1024 * 1024), dC, s3N / 1_000_000, s3B / (1024 * 1024), s3C
      );
    }
    final long rC = bufRefillCount.getAndSet(0), rB = bufRefillBytes.getAndSet(0);
    final long seq = bufSeqRefills.getAndSet(0), seek = bufSeekRefills.getAndSet(0);
    final long dirC = bufDirectCount.getAndSet(0), dirB = bufDirectBytes.getAndSet(0);
    final long pf = prefetchCount.getAndSet(0);
    if (rC + dirC + pf > 0) {   // buffer-fill probe on: refill fill-size + contiguity (over/under), direct, prefetch
      line += StringUtils.safeFormat(
          "  [buf refill %d gets avg %,dB (seq %d, seek %d) | direct %d gets %,dMB | prefetch %d]",
          rC, rC == 0 ? 0 : rB / rC, seq, seek, dirC, dirB / (1024 * 1024), pf
      );
    }
    return line;
  }
}
