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

import io.druid.segment.Segment;
import io.druid.timeline.DataSegment;

import java.io.File;

/**
 */
public interface SegmentLoader
{
  default void done() {}

  File getLocation(DataSegment segment);

  boolean isLoaded(DataSegment segment) throws SegmentLoadingException;

  Segment getSegment(DataSegment segment) throws SegmentLoadingException;

  File getSegmentFiles(DataSegment segment) throws SegmentLoadingException;

  void cleanup(DataSegment segment) throws SegmentLoadingException;

  // --- residency (auto loadMode): the ResidencyManager demotes a hot-cache (tmpfs) segment to range on pressure ---

  /** Build the segment header-first / range (off-heap) if it can range-serve, else the normal segment. */
  default Segment getRangeSegment(DataSegment segment) throws SegmentLoadingException { return getSegment(segment); }

  /** Force the download (tmpfs mmap) residency — used to promote a hot range segment. */
  default Segment getDownloadedSegment(DataSegment segment) throws SegmentLoadingException { return getSegment(segment); }

  /** Bytes currently held in the local (tmpfs) cache; -1 if not tracked (residency sizing then disabled). */
  default long localUsedBytes() { return -1; }

  /** Local (tmpfs) cache budget in bytes; -1 if not tracked. */
  default long localMaxBytes() { return -1; }

  /** True when loadMode=auto — the ServerManager should run the ResidencyManager (pressure demote). */
  default boolean residencyManaged() { return false; }

  /**
   * The resident per-segment value index for query-time segment pruning, or null when pruning is disabled. The
   * ServerManager consults it to skip segments a query's filter provably can't match, before any column fetch.
   */
  default SegmentPruneIndex pruneIndex() { return null; }

  /**
   * The budget/eviction tracker for range-served off-heap column buffers, or null when range residency isn't
   * bounded. The ServerManager notifies it when a segment's query references drain to zero so it can free the
   * segment's buffers deterministically under memory pressure.
   */
  default RangeBufferTracker rangeTracker() { return null; }
}
