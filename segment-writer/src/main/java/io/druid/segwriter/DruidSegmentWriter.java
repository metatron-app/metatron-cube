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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.SegmentUtils;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Builds a single Druid segment from a set of rows entirely in-process — no ingestion task,
 * no Hadoop, no MapReduce — and pushes it to deep storage via a {@link DataSegmentPusher}.
 *
 * Intended to run inside an external engine (e.g. a Spark executor): read your source
 * (parquet, etc.) natively, hand the rows here per partition, and collect the returned
 * {@link DataSegment}s to publish (overlord API) from the driver.
 *
 * Construction uses no Guice; the segment format matches this build's historicals because it
 * uses this build's {@link IndexMergerV9}/{@link IndexIO}.
 */
public final class DruidSegmentWriter
{
  private DruidSegmentWriter() {}

  /**
   * @param spec      schema (datasource, dimensions, metrics, granularity, rollup, ts column)
   * @param interval  the segment's time interval
   * @param version   segment version string (e.g. an ISO timestamp); also the dedup key
   * @param shardSpec partition shard spec (e.g. NoneShardSpec or a hash/linear shard)
   * @param rows      input rows; each must carry the timestamp column (epoch millis as a Number)
   * @param pusher    deep-storage pusher (see {@link DataSegmentPushers})
   * @param tmpDir    scratch dir for the persisted segment
   * @return the published {@link DataSegment} with loadSpec/size/binaryVersion filled in
   */
  public static DataSegment write(
      SegmentSpec spec,
      Interval interval,
      String version,
      ShardSpec shardSpec,
      Iterable<Map<String, Object>> rows,
      DataSegmentPusher pusher,
      File tmpDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    // indexMapper knows the lucene column part serde subtypes, so secondary-indexed columns
    // round-trip through IndexMergerV9/IndexIO (write + read-back).
    final ObjectMapper mapper = Json.indexMapper();
    final IndexIO indexIO = new IndexIO(mapper);
    final IndexMergerV9 merger = new IndexMergerV9(mapper, indexIO);

    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(interval.getStartMillis())
        .withQueryGranularity(spec.getQueryGranularity())
        .withDimensionsSpec(new DimensionsSpec(DimensionsSpec.getDefaultSchemas(spec.getDimensions()), null, null))
        .withMetrics(spec.getMetrics())
        .withRollup(spec.isRollup())
        .build();

    final File persisted;
    final int numRows;
    try (IncrementalIndex index = new OnheapIncrementalIndex(schema, true, Integer.MAX_VALUE)) {
      for (Map<String, Object> row : rows) {
        final Object ts = row.get(spec.getTimestampColumn());
        if (!(ts instanceof Number)) {
          throw new IllegalArgumentException(
              "row timestamp column [" + spec.getTimestampColumn() + "] must be epoch-millis Number, got: " + ts
          );
        }
        index.add(new MapBasedInputRow(((Number) ts).longValue(), spec.getDimensions(), row));
      }
      numRows = index.size();   // post-rollup row count for the segment metadata
      persisted = merger.persist(
          index,
          interval,
          new File(tmpDir, "seg-" + UUID.randomUUID()),
          indexSpec
      );
    }

    final List<String> metricNames = new ArrayList<>();
    for (AggregatorFactory f : spec.getMetrics()) {
      metricNames.add(f.getName());
    }

    final DataSegment template = new DataSegment(
        spec.getDataSource(),
        interval,
        version,
        ImmutableMap.<String, Object>of(),     // loadSpec filled by the pusher
        spec.getDimensions(),
        metricNames,
        shardSpec,
        SegmentUtils.getVersionFromDir(persisted),
        0L,                                     // size filled by the pusher
        numRows
    );
    return pusher.push(persisted, template);
  }
}
