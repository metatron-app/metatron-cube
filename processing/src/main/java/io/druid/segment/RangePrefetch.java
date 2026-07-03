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

import io.druid.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.column.Column;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Parallel column prefetch for range-served (header-first) segments. On a cold range segment each column is a
 * deep-storage range GET, and the scan pulls them one at a time on the query's critical path — N columns = N
 * sequential round-trips. Given the columns a query references ({@code Query.estimatedInitialColumns}), this fires
 * their fetches CONCURRENTLY and returns immediately (no barrier): the column suppliers are memoized single-flight
 * ({@link io.druid.common.guava.DSuppliers}), so the scan's later {@code getColumn(name)} joins the in-flight
 * fetch instead of starting a new one. Fire-and-forget — a failed prefetch just resurfaces as the scan's real
 * fetch, so it can never fail a query. No-op for mmap segments (they aren't range-served).
 */
public class RangePrefetch
{
  private static final Logger log = new Logger(RangePrefetch.class);

  // deep-storage range GETs are I/O-bound, so more threads than cores is fine
  private static final int THREADS = Integer.getInteger("druid.segmentCache.prefetchThreads", 32);
  private static final ExecutorService POOL = Execs.multiThreaded(THREADS, "range-prefetch-%d");

  private RangePrefetch() {}

  /** Kick off concurrent range GETs for the given columns; returns immediately. */
  public static void warm(QueryableIndex index, List<String> columns)
  {
    if (index == null || columns == null || columns.isEmpty()) {
      return;
    }
    for (final String name : columns) {
      POOL.submit(() -> {
        try {
          final Column column = index.getColumn(name);
          if (column != null) {
            column.getNumRows();   // forces the column's range fetch + v9 decode (wrapped or real)
          }
        }
        catch (Throwable t) {
          log.debug("range-prefetch skip column[%s]: %s", name, t.toString());
        }
      });
    }
  }
}
