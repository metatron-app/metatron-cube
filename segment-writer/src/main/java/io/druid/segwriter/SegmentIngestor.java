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

import io.druid.granularity.Granularity;
import io.druid.segment.IndexSpec;
import io.druid.segment.SecondaryIndexingSpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Glue between a {@link SegmentIngestSpec} and {@link DruidSegmentWriter}: builds the per-partition
 * deep-storage pusher and one segment per (interval, shard). Spark calls this from mapPartitions.
 */
public final class SegmentIngestor
{
  private SegmentIngestor() {}

  /** S3 pusher for this spec; credentials come from the default chain (AWS_* env). */
  public static DataSegmentPusher pusher(SegmentIngestSpec spec)
  {
    final boolean smoosh = "s3_smoosh".equals(spec.getStorageType());
    return (smoosh ? DataSegmentPushers.s3Smoosh(
        spec.getBucket(),
        spec.getBaseKey(),
        spec.isDisableAcl(),
        null,                 // accessKey: default credential chain (env)
        null,                 // secretKey
        spec.getEndpoint(),
        spec.getRegion()
    ) : DataSegmentPushers.s3(
        spec.getBucket(),
        spec.getBaseKey(),
        spec.isDisableAcl(),
        null,                 // accessKey: default credential chain (env)
        null,                 // secretKey
        spec.getEndpoint(),
        spec.getRegion()
    ));
  }

  /** The segment interval a timestamp (epoch millis) falls into, per the spec's segmentGranularity. */
  public static Interval bucket(SegmentIngestSpec spec, long timestampMillis)
  {
    return Granularity.fromString(spec.getSegmentGranularity()).bucket(new org.joda.time.DateTime(timestampMillis));
  }

  /** Build + push one segment for a single (interval, shard) partition. */
  public static DataSegment buildSegment(
      SegmentIngestSpec spec,
      Interval interval,
      String version,
      int shardNum,
      int numShards,
      Iterator<Map<String, Object>> rows,
      File tmpDir,
      DataSegmentPusher pusher
  ) throws IOException
  {
    // build secondary indexes (e.g. lucene text) from the raw spec. secondaryIndexing is applied
    // to METRIC columns during merge (IndexMergerV9.setupMetricsWriter), so each indexed column
    // must be declared as a metric (e.g. a {"type":"relay",...,"typeName":"string"} passthrough),
    // NOT a dimension — dimension writers ignore secondaryIndexing.
    final List<String> dimensions = spec.getDimensions();
    IndexSpec indexSpec = IndexSpec.DEFAULT;
    final Map<String, Map<String, Object>> rawSecondary = spec.getSecondaryIndexing();
    if (rawSecondary != null && !rawSecondary.isEmpty()) {
      final Map<String, SecondaryIndexingSpec> secondary = new LinkedHashMap<>();
      for (Map.Entry<String, Map<String, Object>> e : rawSecondary.entrySet()) {
        secondary.put(e.getKey(), Json.indexMapper().convertValue(e.getValue(), SecondaryIndexingSpec.class));
      }
      indexSpec = new IndexSpec(null, null, null, null, secondary, null, false, null);
    }

    final SegmentSpec segmentSpec = new SegmentSpec(
        spec.getDataSource(),
        dimensions,
        spec.getMetrics(),
        Granularity.fromString(spec.getQueryGranularity()),
        spec.isRollup(),
        spec.getTimestampColumn()
    );

    final ShardSpec shardSpec = numShards <= 1 ? NoneShardSpec.instance() : new LinearShardSpec(shardNum);
    // stream the rows straight into the writer (it iterates exactly once) — no intermediate copy
    return DruidSegmentWriter.write(segmentSpec, interval, version, shardSpec, () -> rows, pusher, tmpDir, indexSpec);
  }
}
