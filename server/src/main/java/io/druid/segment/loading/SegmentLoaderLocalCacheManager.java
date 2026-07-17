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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.ContainerHeader;
import io.druid.segment.IndexIO;
import io.druid.segment.LazySegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 */
public class SegmentLoaderLocalCacheManager implements SegmentLoader
{
  private static final Logger log = new Logger(SegmentLoaderLocalCacheManager.class);

  // Boot-fill target for loadMode=auto: fill the tmpfs cache only to this fraction of its budget, not to 100%.
  // Mirrors ResidencyManager.LOW_WM (0.8) — the level the residency policy drains a full cache back down to — so
  // boot lands where the policy wants to sit instead of overshooting to 100% and then demoting for ~an hour. The
  // 0.8..0.9 headroom is left for the policy to promote hot range segments into.
  private static final double AUTO_FILL_TARGET = 0.8;

  private final QueryableIndexFactory factory;
  private final SegmentLoaderConfig config;
  private final ObjectMapper jsonMapper;
  private final IndexIO indexIO;   // only used by the range-serve (header-first) path
  private final RangeBufferTracker rangeTracker;   // null when rangeMaxSize <= 0 (unbounded range residency)
  // Query-time segment pruning: null unless druid.segmentCache.pruneColumns is set. Built eagerly per range
  // segment (dict-only fetch) so a query can skip segments its filter can't match without touching their columns.
  private final SegmentPruneIndex pruneIndex = SegmentPruneIndex.fromSpec(System.getProperty("druid.segmentCache.pruneColumns"));

  private final List<StorageLocation> locations;

  @Inject
  public SegmentLoaderLocalCacheManager(
      QueryableIndexFactory factory,
      @Nullable SegmentLoaderConfig config,
      @Json ObjectMapper mapper,
      IndexIO indexIO
  )
  {
    this.factory = factory;
    this.config = config == null ? new SegmentLoaderConfig() : config;
    this.jsonMapper = mapper;
    this.indexIO = indexIO;
    final RangeDiskCache rangeDiskCache =
        this.config.getRangeMaxSize() > 0
        && this.config.getRangeDiskCachePath() != null && !this.config.getRangeDiskCachePath().isEmpty()
        && this.config.getRangeDiskCacheMaxSize() > 0
        ? new RangeDiskCache(new File(this.config.getRangeDiskCachePath()), this.config.getRangeDiskCacheMaxSize())
        : null;
    this.rangeTracker = this.config.getRangeMaxSize() > 0
        ? new RangeBufferTracker(this.config.getRangeMaxSize(), this.config.getRangeKeepRatio(), rangeDiskCache,
                                 this.config.getRangeMaxLiveIndexes()) : null;

    this.locations = Lists.newArrayList();
    for (StorageLocationConfig locationConfig : this.config.getLocations()) {
      locations.add(new StorageLocation(locationConfig.getPath(), locationConfig.getMaxSize()));
    }
  }

  // back-compat for callers (tests) that don't need the range-serve path
  public SegmentLoaderLocalCacheManager(
      QueryableIndexFactory factory,
      @Nullable SegmentLoaderConfig config,
      @Json ObjectMapper mapper
  )
  {
    this(factory, config, mapper, null);
  }

  public SegmentLoaderLocalCacheManager withConfig(SegmentLoaderConfig config)
  {
    return new SegmentLoaderLocalCacheManager(factory, config, jsonMapper, indexIO);
  }

  @Override
  public boolean isLoaded(final DataSegment segment)
  {
    return findExistingLocation(DataSegmentPusherUtil.getStorageDir(segment)) != null;
  }

  private StorageLocation findExistingLocation(String relativePath)
  {
    for (StorageLocation location : locations) {
      final File localStorageDir = new File(location.getPath(), relativePath);
      if (localStorageDir.exists()) {
        return location;
      }
    }
    return null;
  }

  private StorageLocation allocateStorageLocation(DataSegment segment)
  {
    final Iterator<StorageLocation> iterator = locations.iterator();
    StorageLocation loc = iterator.next();
    while (iterator.hasNext()) {
      loc = loc.mostEmpty(iterator.next());
    }

    if (!loc.canHandle(segment.getSize())) {
      throw new ISE(
          "Segment[%s:%,d] too large for storage[%s:%,d].",
          segment.getIdentifier(), segment.getSize(), loc.getPath(), loc.available()
      );
    }
    return loc;
  }

  @Override
  public Segment getSegment(DataSegment segment) throws SegmentLoadingException
  {
    if (indexIO != null && rangeForSegment(segment)) {
      // Materialize the LoadSpec early (only for a range-served segment) to see whether it can range-serve.
      final LoadSpec loadSpec = jsonMapper.convertValue(segment.getLoadSpec(), LoadSpec.class);
      if (loadSpec instanceof RangeLoadSpec) {
        log.info("Range-serving segment[%s] header-first (no local download)", segment.getIdentifier());
        return rangeSegment(segment, (RangeLoadSpec) loadSpec);
      }
      // otherwise (e.g. an s3_zip segment) fall through to the normal download path
    }

    return downloadSegment(segment);
  }

  /** Force the download (tmpfs mmap) residency — used by the ResidencyManager to promote a hot range segment. */
  @Override
  public Segment getDownloadedSegment(DataSegment segment) throws SegmentLoadingException
  {
    return downloadSegment(segment);
  }

  private Segment downloadSegment(DataSegment segment) throws SegmentLoadingException
  {
    final File segmentFiles = getSegmentFiles(segment);
    final QueryableIndex index = factory.factorize(segmentFiles);
    return new QueryableIndexSegment(index, segment);
  }

  /** Force the range (off-heap, header-first) residency for a segment — used by the ResidencyManager to demote. */
  @Override
  public Segment getRangeSegment(DataSegment segment) throws SegmentLoadingException
  {
    if (indexIO != null) {
      final LoadSpec loadSpec = jsonMapper.convertValue(segment.getLoadSpec(), LoadSpec.class);
      if (loadSpec instanceof RangeLoadSpec) {
        return rangeSegment(segment, (RangeLoadSpec) loadSpec);
      }
    }
    return getSegment(segment);   // can't range (e.g. s3_zip) -> normal load
  }

  @Override
  public long localUsedBytes()
  {
    long used = 0;
    for (StorageLocation loc : locations) {
      used += loc.getMaxSize() - loc.available();
    }
    return used;
  }

  @Override
  public long localMaxBytes()
  {
    long max = 0;
    for (StorageLocation loc : locations) {
      max += loc.getMaxSize();
    }
    return max;
  }

  @Override
  public boolean residencyManaged()
  {
    return "auto".equalsIgnoreCase(config.getLoadMode());
  }

  /**
   * Whether this segment should be served header-first / range (off-heap direct buffers) vs downloaded to the
   * local cache (mmap — RAM when the cache is on tmpfs). Driven by {@code druid.segmentCache.loadMode}:
   *   download (default) - never range;  range - always range;
   *   split              - deterministic ~50/50 by identifier, to run BOTH residencies on one node (A/B);
   *   auto               - fill the local (tmpfs) cache greedily; overflow goes range (off-heap direct). The
   *                        residency policy seam — segment priority (recency) + live headroom plug in here.
   * The caller already gated on range-capability (RangeLoadSpec), so a false here means "download".
   */
  private boolean rangeForSegment(DataSegment segment)
  {
    final String mode = config.getLoadMode();
    if ("range".equalsIgnoreCase(mode)) {
      return true;
    }
    if ("split".equalsIgnoreCase(mode)) {
      return (segment.getIdentifier().hashCode() & 1) == 0;
    }
    if ("auto".equalsIgnoreCase(mode)) {
      // Greedy up to a FILL TARGET (not 100%): keep the segment resident in the local cache (tmpfs mmap) while the
      // cache is under AUTO_FILL_TARGET of its budget; overflow serves header-first from off-heap direct buffers.
      // Stopping at ~80% (the residency low watermark) means boot settles where the policy wants instead of
      // overshooting to 100% and demoting the excess for ~an hour, and leaves headroom for the policy to promote
      // hot range segments. (Recency-optimal placement of that headroom is the ResidencyManager's job.)
      return localUsedBytes() + segment.getSize() > AUTO_FILL_TARGET * localMaxBytes();
    }
    return false;   // "download"
  }

  /**
   * Header-first, disk-less load: return a {@link LazySegment} whose QueryableIndex is built (once, memoized)
   * from the header bundle + a range-fetch mapper. Nothing is fetched until the first query touches the segment;
   * columns are then range-read from deep storage on demand, and each column is memoized for the segment's life.
   */
  private Segment rangeSegment(final DataSegment segment, final RangeLoadSpec spec)
  {
    // Build the memoized v9 index from a range fetcher. v2: fully-readable header carries capabilities -> only
    // queried columns are fetched. v1: legacy packed header (version.bin+meta.smoosh+index.drd+metadata.drd).
    final Function<SmooshedFileMapper.RangeFetcher, QueryableIndex> build = fetcher -> {
      try {
        final byte[] header = spec.header();
        return ContainerHeader.isV2(header)
               ? ContainerHeader.load(header, fetcher, jsonMapper)
               : indexIO.loadIndex(null, false, SmooshedFileMapper.fromHeader(header, fetcher));
      }
      catch (IOException e) {
        throw new RuntimeException("range load failed for segment[" + segment.getIdentifier() + "]", e);
      }
    };
    // Eagerly build the prune-index value sets (dict-only fetch) so pruning works from the first query, before any
    // column is materialized. Best-effort: never fails the load. Costs one header + one dict ranged GET per segment.
    if (pruneIndex != null && !pruneIndex.has(segment.getIdentifier())) {
      try {
        pruneIndex.index(segment, spec.header(), spec.rangeFetcher());
      }
      catch (Exception e) {
        log.warn(e, "prune-index build skipped for segment[%s]", segment.getIdentifier());
      }
    }

    // When rangeMaxSize is set, route through the tracker (accounted, evictable direct buffers); else legacy
    // unbounded memoize.
    final Supplier<QueryableIndex> loader = rangeTracker != null
        ? rangeTracker.track(segment, build, spec::rangeFetcher)
        : Suppliers.memoize(() -> build.apply(spec.rangeFetcher()));
    return new LazySegment(segment, loader);
  }

  @Override
  public SegmentPruneIndex pruneIndex()
  {
    return pruneIndex;
  }

  @Override
  public RangeBufferTracker rangeTracker()
  {
    return rangeTracker;
  }

  @Override
  public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
  {
    final String relativePath = DataSegmentPusherUtil.getStorageDir(segment);

    final File storageDir;

    StorageLocation loc = findExistingLocation(relativePath);
    if (loc == null) {
      loc = allocateStorageLocation(segment);

      storageDir = new File(loc.getPath(), relativePath);

      // We use a marker to prevent the case where a segment is downloaded, but before the download completes,
      // the parent directories of the segment are removed
      final File downloadStartMarker = new File(storageDir, "downloadStartMarker");
      synchronized (loc) {
        if (!storageDir.mkdirs()) {
          log.debug("Unable to make parent file[%s]", storageDir);
        }
        try {
          if (!downloadStartMarker.createNewFile()) {
            throw new SegmentLoadingException("Was not able to create new download marker for [%s]", storageDir);
          }
        }
        catch (IOException e) {
          throw new SegmentLoadingException(e, "Unable to create marker file for [%s]", storageDir);
        }
      }

      // LoadSpec isn't materialized until here so that any system can interpret Segment without having to have all the LoadSpec dependencies.
      final LoadSpec loadSpec = jsonMapper.convertValue(segment.getLoadSpec(), LoadSpec.class);
      final LoadSpec.LoadSpecResult result = loadSpec.loadSegment(storageDir);
      if (result.getSize() != segment.getSize()) {
        log.warn(
            "Segment [%s] is different than expected size. Expected [%d] found [%d]",
            segment.getIdentifier(), segment.getSize(), result.getSize()
        );
      }

      if (!downloadStartMarker.delete()) {
        throw new SegmentLoadingException("Unable to remove marker file for [%s]", storageDir);
      }
    } else {
      storageDir = new File(loc.getPath(), relativePath);
    }

    loc.addSegment(segment);

    return storageDir;
  }

  @Override
  public File getLocation(DataSegment segment)
  {
    final String relativePath = DataSegmentPusherUtil.getStorageDir(segment);
    final StorageLocation location = findExistingLocation(relativePath);
    if (location != null) {
      final File file = new File(location.getPath(), relativePath);
      if (file.exists()) {
        return file;
      }
    }
    return null;
  }

  @Override
  public void done()
  {
    if (!config.isSyncOnStart()) {
      return;
    }
    for (StorageLocation location : locations) {
      for (File dsp : location.getPath().listFiles()) {
        if (dsp.isDirectory()) {
          String ds = dsp.getName();
          File[] dspc = dsp.listFiles();
          if (dspc == null || dspc.length == 0) {
            FileUtils.deleteQuietly(dsp);
            log.info("Deleted empty path [%s]", ds);
            continue;
          }
          for (File itvp : dspc) {
            if (itvp.isDirectory()) {
              Interval interval = DataSegmentPusherUtil.parseInterval(itvp.getName());
              if (interval != null) {
                File[] itvpc = itvp.listFiles();
                if (itvpc == null || itvpc.length == 0) {
                  FileUtils.deleteQuietly(itvp);
                  log.info("Deleted empty path [%s/%s]", ds, itvp.getName());
                  continue;
                }
                for (File vp : itvpc) {
                  if (vp.isDirectory()) {
                    String version = vp.getName();
                    File[] vpc = vp.listFiles();
                    if (vpc == null || vpc.length == 0) {
                      FileUtils.deleteQuietly(vp);
                      log.info("Deleted empty path [%s/%s/%s]", ds, itvp.getName(), version);
                      continue;
                    }
                    for (File pp : vpc) {
                      Integer p = Ints.tryParse(pp.getName());
                      if (p != null && pp.isDirectory()) {
                        String identifier = DataSegment.toSegmentId(ds, interval, version, p);
                        if (!location.contains(DataSegment.asKey(identifier)) && FileUtils.deleteQuietly(pp)) {
                          log.info("Deleted remains of segment [%s]", identifier);
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  @Override
  public void cleanup(DataSegment segment) throws SegmentLoadingException
  {
    if (!config.isDeleteOnRemove()) {
      return;
    }

    final String relativePath = DataSegmentPusherUtil.getStorageDir(segment);
    final StorageLocation loc = findExistingLocation(relativePath);

    if (loc == null) {
      log.debug("Asked to cleanup something[%s] that didn't exist.  Skipping.", segment);
      return;
    }

    try {
      // Druid creates folders of the form dataSource/interval/version/partitionNum.
      // We need to clean up all these directories if they are all empty.
      cleanupCacheFiles(loc, new File(loc.getPath(), relativePath));
      loc.removeSegment(segment);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  private void cleanupCacheFiles(final StorageLocation location, final File cacheFile) throws IOException
  {
    if (cacheFile.equals(location.getPath())) {
      return;
    }

    synchronized (location) {
      log.debug("Deleting directory[%s]", cacheFile);
      try {
        FileUtils.deleteDirectory(cacheFile);
      }
      catch (Exception e) {
        log.error("Unable to remove file[%s]", cacheFile);
      }
    }

    final File parent = cacheFile.getParentFile();
    if (parent != null && parent.listFiles().length == 0) {
      cleanupCacheFiles(location, parent);
    }
  }
}
