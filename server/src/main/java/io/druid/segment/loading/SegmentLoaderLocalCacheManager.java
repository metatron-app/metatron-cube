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
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
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

  private final QueryableIndexFactory factory;
  private final SegmentLoaderConfig config;
  private final ObjectMapper jsonMapper;

  private final List<StorageLocation> locations;

  @Inject
  public SegmentLoaderLocalCacheManager(
      QueryableIndexFactory factory,
      @Nullable SegmentLoaderConfig config,
      @Json ObjectMapper mapper
  )
  {
    this.factory = factory;
    this.config = config == null ? new SegmentLoaderConfig() : config;
    this.jsonMapper = mapper;

    this.locations = Lists.newArrayList();
    for (StorageLocationConfig locationConfig : this.config.getLocations()) {
      locations.add(new StorageLocation(locationConfig.getPath(), locationConfig.getMaxSize()));
    }
  }

  public SegmentLoaderLocalCacheManager withConfig(SegmentLoaderConfig config)
  {
    return new SegmentLoaderLocalCacheManager(factory, config, jsonMapper);
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
    final File segmentFiles = getSegmentFiles(segment);
    final QueryableIndex index = factory.factorize(segmentFiles);

    return new QueryableIndexSegment(index, segment);
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
