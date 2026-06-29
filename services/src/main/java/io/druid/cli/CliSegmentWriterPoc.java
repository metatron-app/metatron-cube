/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.SegmentUtils;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.server.DruidNode;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * PoC: build a Druid segment fully in-process (no ingestion task, no Hadoop/MR),
 * push it to the configured deep storage, and publish it to the metadata store.
 *
 * This is the standalone "segment writer" path that a Spark job would call per
 * partition: IncrementalIndex -> IndexMergerV9.persist -> DataSegmentPusher ->
 * announceHistoricalSegments. A historical then loads the segment from deep
 * storage and serves queries on it.
 *
 * NOTE on publishing: this drives IndexerMetadataStorageCoordinator in-process
 * (exactly what a task's SegmentInsertAction calls underneath). A distributed
 * Spark job would instead POST the built DataSegment(s) to the overlord; the
 * overlord's /druid/indexer/v1/action (SegmentInsertAction) is lock-gated, so a
 * thin publish endpoint or a lightweight publishing task is the production path.
 */
@Command(
    name = "segment-writer-poc",
    description = "PoC: build a segment in-process, push to deep storage, and publish to metadata"
)
public class CliSegmentWriterPoc extends GuiceRunnable
{
  private static final Logger log = new Logger(CliSegmentWriterPoc.class);

  @Option(name = "--dataSource", description = "target datasource name")
  private String dataSource = "poc_ds";

  @Option(name = "--interval", description = "segment interval (ISO)")
  private String intervalString = "2020-01-01/2020-01-02";

  public CliSegmentWriterPoc()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder, Key.get(DruidNode.class, Self.class), new DruidNode("tools", "localhost", -1)
            );
          }
        }
    );
  }

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final ObjectMapper mapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final DataSegmentPusher pusher = injector.getInstance(DataSegmentPusher.class);
    final IndexMergerV9 merger = injector.getInstance(IndexMergerV9.class);
    final IndexerMetadataStorageCoordinator coordinator =
        injector.getInstance(IndexerMetadataStorageCoordinator.class);

    final Interval interval = new Interval(intervalString);
    final List<String> dimensions = Arrays.asList("country", "page");
    final List<String> metrics = Arrays.asList("rows");

    try {
      // 1) build an in-memory index
      final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
          .withMinTimestamp(interval.getStartMillis())
          .withQueryGranularity(QueryGranularities.NONE)
          .withDimensionsSpec(new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dimensions), null, null))
          .withMetrics(new CountAggregatorFactory("rows"))
          .build();

      final File outDir;
      try (IncrementalIndex index = new OnheapIncrementalIndex(schema, true, 1_000_000)) {
        final long t = interval.getStartMillis();
        index.add(row(t, "KR", "Seoul"));
        index.add(row(t + 1000, "KR", "Busan"));
        index.add(row(t + 2000, "US", "NYC"));
        index.add(row(t + 3000, "US", "NYC"));
        index.add(row(t + 4000, "JP", "Tokyo"));

        // 2) persist to the local segment format
        outDir = merger.persist(
            index,
            interval,
            Files.createTempDir(),
            IndexSpec.DEFAULT
        );
      }

      // 3) push to deep storage (loadSpec/size/binaryVersion filled by the pusher)
      final DataSegment template = new DataSegment(
          dataSource,
          interval,
          new DateTime().toString(),
          ImmutableMap.<String, Object>of(),
          dimensions,
          metrics,
          NoneShardSpec.instance(),
          SegmentUtils.getVersionFromDir(outDir),
          0L
      );
      final DataSegment pushed = pusher.push(outDir, template);
      log.info("Pushed segment [%s] loadSpec=%s", pushed.getIdentifier(), mapper.writeValueAsString(pushed.getLoadSpec()));

      // 4) publish to the metadata store (what the overlord does internally)
      coordinator.announceHistoricalSegments(ImmutableSet.of(pushed));
      log.info("Published segment [%s] to metadata. Coordinator will assign it; query datasource [%s].",
               pushed.getIdentifier(), dataSource);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static MapBasedInputRow row(long ts, String country, String page)
  {
    final Map<String, Object> event = ImmutableMap.<String, Object>of("country", country, "page", page);
    return new MapBasedInputRow(ts, Arrays.asList("country", "page"), event);
  }
}
