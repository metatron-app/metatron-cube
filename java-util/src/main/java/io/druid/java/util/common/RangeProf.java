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

  public static final AtomicLong openNanos = new AtomicLong();
  public static final AtomicLong openCount = new AtomicLong();

  public static final AtomicLong searchNanos = new AtomicLong();
  public static final AtomicLong searchCount = new AtomicLong();

  private RangeProf() {}

  /** Snapshot the current counters, resetting them to zero, and render a one-line summary. */
  public static String snapshotAndReset()
  {
    final long fN = fetchNanos.getAndSet(0), fB = fetchBytes.getAndSet(0), fC = fetchCount.getAndSet(0);
    final long oN = openNanos.getAndSet(0), oC = openCount.getAndSet(0);
    final long sN = searchNanos.getAndSet(0), sC = searchCount.getAndSet(0);
    final double total = Math.max(1, fN + oN + sN);
    return StringUtils.safeFormat(
        "fetch %,dms (%,dMB, %d gets, %.0f%%) | open %,dms (%d, %.0f%%) | search %,dms (%d, %.0f%%)",
        fN / 1_000_000, fB / (1024 * 1024), fC, 100 * fN / total,
        oN / 1_000_000, oC, 100 * oN / total,
        sN / 1_000_000, sC, 100 * sN / total
    );
  }
}
