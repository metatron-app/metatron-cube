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

package io.druid.segwriter;

import com.google.common.collect.ImmutableMap;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;

/**
 * Minimal local-filesystem pusher (zips the segment dir to baseDir/&lt;storageDir&gt;/index.zip).
 * Self-contained so the writer module doesn't depend on druid-server. Mainly for tests/local
 * runs; real (Spark) deployments use {@link DataSegmentPushers#s3}.
 */
public class LocalDataSegmentPusher implements DataSegmentPusher
{
  private final File baseDir;

  public LocalDataSegmentPusher(File baseDir)
  {
    this.baseDir = baseDir;
  }

  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public String getPathForHadoop()
  {
    return baseDir.getAbsolutePath();
  }

  @Override
  public DataSegment push(File segmentDir, DataSegment segment) throws IOException
  {
    final File outDir = new File(baseDir, DataSegmentPusherUtil.getStorageDir(segment));
    outDir.mkdirs();
    final File indexZip = new File(outDir, "index.zip");
    final long size = CompressionUtils.zip(segmentDir, indexZip);
    return segment.withLoadSpec(ImmutableMap.<String, Object>of("type", "local", "path", indexZip.getAbsolutePath()))
                  .withSize(size)
                  .withBinaryVersion(SegmentUtils.getVersionFromDir(segmentDir));
  }
}
