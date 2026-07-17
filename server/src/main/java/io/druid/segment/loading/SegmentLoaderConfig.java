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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 */
public class SegmentLoaderConfig
{
  @JsonProperty
  private List<StorageLocationConfig> locations = Arrays.asList();

  @JsonProperty("syncOnStart")
  private boolean syncOnStart = false;

  @JsonProperty("deleteOnRemove")
  private boolean deleteOnRemove = true;

  @JsonProperty("dropSegmentDelayMillis")
  private int dropSegmentDelayMillis = 30 * 1000; // 30 seconds

  @JsonProperty("announceIntervalMillis")
  private int announceIntervalMillis = 0; // do not background announce

  @JsonProperty("numLoadingThreads")
  private int numLoadingThreads = 1;

  @JsonProperty("numBootstrapThreads")
  private Integer numBootstrapThreads = null;

  @JsonProperty("reportFileNotFoundIntervalMillis")
  private int reportFileNotFoundIntervalMillis = 0;

  // How to serve a segment whose LoadSpec is range-capable (e.g. s3_smoosh):
  //   "download" (default) - pull the whole segment to a local dir and mmap it (legacy behaviour)
  //   "range"              - header-first: keep nothing on local disk, fetch column byte-ranges from deep
  //                          storage on first query (see RangeLoadSpec / SmooshedFileMapper.fromHeader).
  // Left as a string (not a boolean) so it can grow into a richer policy (per-datasource/tier/size) later.
  @JsonProperty("loadMode")
  private String loadMode = "download";

  // Budget (bytes) for direct off-heap memory held by range-served (header-first) segment columns. When > 0 the
  // range loader tracks each materialized segment's fetched-column bytes and evicts the coldest (dematerialize:
  // drop the memoized index so its direct buffers are freed, keeping the segment queryable) to stay under budget
  // — bounds a wide scan that would otherwise pin every touched segment's columns. <= 0 disables (unbounded,
  // legacy). Size it under -XX:MaxDirectMemorySize minus the processing-buffer reserve.
  @JsonProperty("rangeMaxSize")
  private long rangeMaxSize = 0;

  // range residency: below this fraction of rangeMaxSize, an idle segment's fetched column buffers are KEPT (warm
  // cache); at or above it, they are freed deterministically the moment a query releases the segment (no Cleaner
  // lag). 1.0 = never free on release (legacy lazy behavior); 0.0 = always free on release (no cache).
  @JsonProperty("rangeKeepRatio")
  private double rangeKeepRatio = 0.5;

  // Optional local-disk WARM tier under the direct-buffer (rangeMaxSize) tier: when rangeDiskCachePath is set and
  // rangeDiskCacheMaxSize > 0, each fetched column byte-range is written through to a per-segment sparse file on
  // local disk. A later fetch of the same range (e.g. after its direct buffer was evicted) is mmap'd from disk —
  // page cache, NOT counted against -XX:MaxDirectMemorySize — instead of re-fetched from deep storage. Only fetched
  // (queried) columns are ever written, so unlike loadMode=download the fat unqueried columns are never localized.
  // Path MUST be a real disk volume (not tmpfs, not the pod overlay root). Cleared on boot (no cross-restart reuse).
  @JsonProperty("rangeDiskCachePath")
  private String rangeDiskCachePath = null;

  @JsonProperty("rangeDiskCacheMaxSize")
  private long rangeDiskCacheMaxSize = 0;

  // Cap on the number of range segments kept MEMOIZED (their QueryableIndex held, so a warm query skips the
  // DirectoryReader.open / header parse). Bounds heap: when live > this, the coldest are dematerialized (re-opened
  // from disk/deep-storage on next access). Separate from rangeMaxSize, which bounds direct-memory BYTES; this
  // bounds the heap held by the memoized index objects (a disk-served segment adds ~0 direct bytes, so the byte
  // budget alone never reclaims it). With a disk cache this is what lets warm repeats skip open.
  @JsonProperty("rangeMaxLiveIndexes")
  private int rangeMaxLiveIndexes = 20000;

  @JsonProperty
  private File infoDir = null;

  public List<StorageLocationConfig> getLocations()
  {
    return locations;
  }

  public boolean isSyncOnStart()
  {
    return syncOnStart;
  }

  public boolean isDeleteOnRemove()
  {
    return deleteOnRemove;
  }

  public int getDropSegmentDelayMillis()
  {
    return dropSegmentDelayMillis;
  }

  public int getAnnounceIntervalMillis()
  {
    return announceIntervalMillis;
  }

  public int getReportFileNotFoundIntervalMillis()
  {
    return reportFileNotFoundIntervalMillis;
  }

  public String getLoadMode()
  {
    return loadMode;
  }

  public long getRangeMaxSize()
  {
    return rangeMaxSize;
  }

  public double getRangeKeepRatio()
  {
    return rangeKeepRatio;
  }

  public String getRangeDiskCachePath()
  {
    return rangeDiskCachePath;
  }

  public long getRangeDiskCacheMaxSize()
  {
    return rangeDiskCacheMaxSize;
  }

  public int getRangeMaxLiveIndexes()
  {
    return rangeMaxLiveIndexes;
  }

  /** true when range-capable segments should be served header-first (no local download). */
  public boolean isRangeServe()
  {
    return "range".equalsIgnoreCase(loadMode);
  }

  public int getNumLoadingThreads()
  {
    return numLoadingThreads;
  }

  public int getNumBootstrapThreads() {
    return numBootstrapThreads == null ? numLoadingThreads : numBootstrapThreads;
  }

  public File getInfoDir()
  {
    if (infoDir == null) {
      infoDir = new File(locations.get(0).getPath(), "info_dir");
    }

    return infoDir;
  }

  public SegmentLoaderConfig withLocations(List<StorageLocationConfig> locations)
  {
    SegmentLoaderConfig retVal = new SegmentLoaderConfig();
    retVal.locations = Lists.newArrayList(locations);
    retVal.deleteOnRemove = this.deleteOnRemove;
    retVal.reportFileNotFoundIntervalMillis = this.reportFileNotFoundIntervalMillis;
    retVal.infoDir = this.infoDir;
    return retVal;
  }

  @Override
  public String toString()
  {
    return "SegmentLoaderConfig{" +
           "locations=" + locations +
           ", deleteOnRemove=" + deleteOnRemove +
           ", reportFileNotFoundIntervalMillis=" + reportFileNotFoundIntervalMillis +
           ", dropSegmentDelayMillis=" + dropSegmentDelayMillis +
           ", infoDir=" + infoDir +
           '}';
  }
}
