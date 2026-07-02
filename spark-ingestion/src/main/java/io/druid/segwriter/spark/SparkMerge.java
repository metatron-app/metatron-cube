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

package io.druid.segwriter.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.SecondaryIndexingSpec;
import io.druid.segment.SegmentUtils;
import io.druid.segwriter.DataSegmentPushers;
import io.druid.segwriter.Json;
import io.druid.segwriter.SegmentPublisher;
import io.druid.storage.s3.S3Clients;
import io.druid.storage.s3.S3DataSegmentPuller;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One-off segment merge as a Spark job: pull a set of source segments from deep storage, physically merge
 * them with this build's {@link IndexMergerV9} (so index-only lucene columns merge via addIndexes rather
 * than being rebuilt from absent rows), and publish the result under a target datasource.
 *
 * Intended for the shard-disjoint case: the source segments must be time-disjoint (one per interval), so the
 * merge is an in-order concat and the lucene docID == row-ordinal identity is preserved. Overlapping sources
 * make IndexMergerV9 throw (sharded merge is not supported yet).
 *
 *   spark-submit --class io.druid.segwriter.spark.SparkMerge druid-spark-ingestion-*-spark.jar /path/to/merge-spec.json
 */
public final class SparkMerge
{
  public static void main(String[] args) throws Exception
  {
    if (args.length < 1) {
      throw new IllegalArgumentException("usage: SparkMerge <merge-spec.json>");
    }
    final ObjectMapper mapper = Json.indexMapper();
    final JsonNode spec = mapper.readTree(new File(args[0]));

    final String targetDataSource = spec.get("dataSource").asText();
    final String sourceDataSource = spec.get("sourceDataSource").asText();
    final String coordinatorUrl = spec.get("coordinatorUrl").asText();
    final Interval interval = new Interval(spec.get("interval").asText());
    final List<String> dimensions = new ArrayList<>();
    for (JsonNode d : spec.get("dimensions")) {
      dimensions.add(d.asText());
    }

    // deep-storage (seaweed) creds — same AWS_* env the ingestion uses to push
    final String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    final String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    final String endpoint = spec.get("endpoint").asText();

    // spark session: the operator launches us as a Spark app; the merge itself runs on the driver
    final SparkSession spark = SparkSession.builder().appName("druid-segment-merge").getOrCreate();
    try {
      // 1) fetch source segment metadata (loadSpec) from the coordinator, keep those inside the interval
      final String segsJson = httpGet(
          coordinatorUrl + "/druid/coordinator/v1/metadata/datasources/" + sourceDataSource + "/segments?full"
      );
      final List<DataSegment> sources = new ArrayList<>();
      for (JsonNode node : mapper.readTree(segsJson)) {
        final DataSegment segment = mapper.treeToValue(node, DataSegment.class);
        if (interval.contains(segment.getInterval())) {
          sources.add(segment);
        }
      }
      if (sources.isEmpty()) {
        throw new IllegalStateException("no " + sourceDataSource + " segments inside " + interval);
      }
      System.out.println("druid-spark-merge: merging " + sources.size() + " segment(s) of " + sourceDataSource
                         + " in " + interval + " -> " + targetDataSource);

      // 2) pull each segment from deep storage and open it
      final S3Client s3 = S3Clients.create(accessKey, secretKey, endpoint, null);
      final S3DataSegmentPuller puller = new S3DataSegmentPuller(s3);
      final IndexIO indexIO = new IndexIO(mapper);
      final File work = Files.createTempDir();
      final List<QueryableIndex> indexes = new ArrayList<>();
      Interval merged = null;
      for (DataSegment segment : sources) {
        final File dir = new File(work, "src-" + segment.getShardSpec().getPartitionNum());
        puller.getSegmentFiles(segment, dir);
        indexes.add(indexIO.loadIndex(dir));
        merged = merged == null ? segment.getInterval() : mergeInterval(merged, segment.getInterval());
      }

      // 3) build IndexSpec carrying the same secondary index (index-only lucene on `raw`) so the merge
      //    routes through IndexMergerV9's addIndexes path instead of rebuilding from (absent) rows
      final IndexSpec indexSpec = toIndexSpec(mapper, spec.get("secondaryIndexing"));
      final AggregatorFactory[] metricAggs = indexes.get(0).getMetadata() == null
                                             ? null : indexes.get(0).getMetadata().getAggregators();

      // 4) merge (rollup=false, time-disjoint concat)
      final File out = new File(work, "merged");
      final IndexMergerV9 merger = new IndexMergerV9(mapper, indexIO);
      final File mergedDir = merger.mergeQueryableIndex(indexes, false, metricAggs, out, indexSpec);

      // 5) push to deep storage + publish under the target datasource
      final List<String> metricNames = new ArrayList<>();
      for (AggregatorFactory f : metricAggs == null ? new AggregatorFactory[0] : metricAggs) {
        metricNames.add(f.getName());
      }
      final DataSegment template = new DataSegment(
          targetDataSource,
          merged,
          new DateTime().toString(),                 // version
          new LinkedHashMap<>(),                      // loadSpec filled by the pusher
          dimensions,
          metricNames,
          LinearShardSpec.of(0),
          SegmentUtils.getVersionFromDir(mergedDir),
          0L,
          countRows(indexes)
      );
      final DataSegment published = DataSegmentPushers.s3(
          spec.get("bucket").asText(),
          spec.get("baseKey").asText(),
          spec.path("disableAcl").asBoolean(true),
          accessKey,
          secretKey,
          endpoint,
          null
      ).push(mergedDir, template);

      final int n = SegmentPublisher.publish(spec.get("publishUrl").asText(), java.util.Collections.singletonList(published), mapper);
      System.out.println("druid-spark-merge: published " + n + " merged segment for " + targetDataSource
                         + " " + merged + " (" + published.getSize() + " bytes, " + published.getNumRows() + " rows)");
    }
    finally {
      spark.stop();
    }
  }

  private static IndexSpec toIndexSpec(ObjectMapper mapper, JsonNode secondaryIndexing)
  {
    if (secondaryIndexing == null || secondaryIndexing.isNull() || secondaryIndexing.size() == 0) {
      return IndexSpec.DEFAULT;
    }
    final Map<String, SecondaryIndexingSpec> secondary = new LinkedHashMap<>();
    secondaryIndexing.fields().forEachRemaining(
        e -> secondary.put(e.getKey(), mapper.convertValue(e.getValue(), SecondaryIndexingSpec.class))
    );
    return new IndexSpec(null, null, null, null, secondary, null, false, null);
  }

  private static Interval mergeInterval(Interval a, Interval b)
  {
    final long start = Math.min(a.getStartMillis(), b.getStartMillis());
    final long end = Math.max(a.getEndMillis(), b.getEndMillis());
    return new Interval(start, end);
  }

  private static int countRows(List<QueryableIndex> indexes)
  {
    int rows = 0;
    for (QueryableIndex index : indexes) {
      rows += index.getNumRows();
    }
    return rows;
  }

  private static String httpGet(String url) throws Exception
  {
    final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30_000);
    conn.setReadTimeout(120_000);
    try (InputStream in = conn.getInputStream()) {
      return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
    finally {
      conn.disconnect();
    }
  }

  private SparkMerge() {}
}
