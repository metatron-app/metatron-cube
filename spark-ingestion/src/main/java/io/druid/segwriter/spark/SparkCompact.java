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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Compact a sharded index-only datasource entirely in Spark. Each interval (e.g. an hour) holds up to N
 * shards; a normal per-interval merge would overlap them and fail. Instead we bin-pack "one shard per interval
 * into each group" so every group is TIME-DISJOINT, then physically merge each group with this build's
 * {@link IndexMergerV9} (index-only lucene columns merge via addIndexes, no re-analysis). Because the sources in
 * a group are time-disjoint, the merge is an in-order concat and docID == row-ordinal identity is preserved -- no
 * permutation needed. Output = up to max(shards-per-interval) full-range segments, which (at a newer version)
 * overshadow the original per-interval shards.
 *
 * Groups are merged in parallel across executors; the driver bulk-publishes the result atomically.
 *
 *   spark-submit --class io.druid.segwriter.spark.SparkCompact druid-spark-ingestion-*-spark.jar /path/to/compact-spec.json
 */
public final class SparkCompact
{
  public static void main(String[] args) throws Exception
  {
    if (args.length < 1) {
      throw new IllegalArgumentException("usage: SparkCompact <compact-spec.json>");
    }
    final ObjectMapper mapper = Json.indexMapper();
    final JsonNode spec = mapper.readTree(new File(args[0]));

    final String targetDataSource = spec.get("dataSource").asText();
    final String sourceDataSource = spec.get("sourceDataSource").asText();
    final String coordinatorUrl = spec.get("coordinatorUrl").asText();
    final Interval interval = new Interval(spec.get("interval").asText());
    final String endpoint = spec.get("endpoint").asText();
    final String bucket = spec.get("bucket").asText();
    final String baseKey = spec.get("baseKey").asText();
    final boolean disableAcl = spec.path("disableAcl").asBoolean(true);
    final String publishUrl = spec.get("publishUrl").asText();
    final String secondaryIndexingJson = mapper.writeValueAsString(spec.get("secondaryIndexing"));
    final List<String> dimensions = new ArrayList<>();
    for (JsonNode d : spec.get("dimensions")) {
      dimensions.add(d.asText());
    }
    final String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    final String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    final String version = new DateTime().toString();   // one version for the whole compaction

    final SparkSession spark = SparkSession.builder().appName("druid-segment-compact").getOrCreate();
    try (JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
      // 1) driver: list source segments in range, bucket them by interval
      final String segsJson = httpGet(
          coordinatorUrl + "/druid/coordinator/v1/metadata/datasources/" + sourceDataSource + "/segments?full"
      );
      final Map<String, List<DataSegment>> byInterval = new LinkedHashMap<>();
      for (JsonNode node : mapper.readTree(segsJson)) {
        final DataSegment segment = mapper.treeToValue(node, DataSegment.class);
        if (interval.contains(segment.getInterval())) {
          byInterval.computeIfAbsent(segment.getInterval().toString(), k -> new ArrayList<>()).add(segment);
        }
      }
      if (byInterval.isEmpty()) {
        throw new IllegalStateException("no " + sourceDataSource + " segments inside " + interval);
      }
      // 2) balanced bin-pack under the disjoint constraint (<=1 shard per interval per group). An interval
      //    with N shards must span N distinct groups, so we need maxShards groups. For each interval (most
      //    constrained first) assign its shards to the currently least-loaded groups, biggest shard onto the
      //    smallest group, so the output segments come out evenly sized -- no permutation needed.
      int maxShards = 0;
      for (List<DataSegment> shards : byInterval.values()) {
        maxShards = Math.max(maxShards, shards.size());
      }
      final long[] load = new long[maxShards];
      final List<List<DataSegment>> bins = new ArrayList<>();
      for (int g = 0; g < maxShards; g++) {
        bins.add(new ArrayList<>());
      }
      final List<List<DataSegment>> intervals = new ArrayList<>(byInterval.values());
      intervals.sort(Comparator.comparingInt((List<DataSegment> s) -> s.size()).reversed());
      for (List<DataSegment> shards : intervals) {
        shards.sort(Comparator.comparingLong(DataSegment::getSize).reversed());   // biggest shard first
        final Integer[] order = new Integer[maxShards];
        for (int i = 0; i < maxShards; i++) {
          order[i] = i;
        }
        java.util.Arrays.sort(order, Comparator.comparingLong(i -> load[i]));      // least-loaded first
        for (int i = 0; i < shards.size(); i++) {
          final int g = order[i];
          bins.get(g).add(shards.get(i));
          load[g] += shards.get(i).getSize();
        }
      }
      final List<List<String>> groups = new ArrayList<>();
      for (List<DataSegment> bin : bins) {
        if (bin.isEmpty()) {
          continue;
        }
        final List<String> js = new ArrayList<>();
        for (DataSegment s : bin) {
          js.add(mapper.writeValueAsString(s));   // ship as JSON (DataSegment closures)
        }
        groups.add(js);
      }
      System.out.println("druid-spark-compact: " + byInterval.size() + " intervals, up to " + maxShards
                         + " shards -> " + groups.size() + " group(s) of " + sourceDataSource + " -> " + targetDataSource);

      // 3) merge each group on an executor (time-disjoint concat), tagging the output shard with the group index
      final List<String> mergedJson = jsc.parallelize(groups, groups.size()).zipWithIndex().map(pair -> {
        final List<String> group = pair._1();
        final int shard = pair._2().intValue();
        final ObjectMapper m = Json.indexMapper();
        final IndexIO indexIO = new IndexIO(m);
        final S3Client s3 = S3Clients.create(accessKey, secretKey, endpoint, null);
        final S3DataSegmentPuller puller = new S3DataSegmentPuller(s3);
        final File work = Files.createTempDir();
        final List<QueryableIndex> indexes = new ArrayList<>();
        Interval merged = null;
        for (String segJson : group) {
          final DataSegment source = m.readValue(segJson, DataSegment.class);
          final File dir = new File(work, "src-" + source.getShardSpec().getPartitionNum());
          puller.getSegmentFiles(source, dir);
          indexes.add(indexIO.loadIndex(dir));
          merged = merged == null ? source.getInterval() : span(merged, source.getInterval());
        }
        final IndexSpec indexSpec = toIndexSpec(m, secondaryIndexingJson);
        final AggregatorFactory[] metricAggs = indexes.get(0).getMetadata() == null
                                               ? null : indexes.get(0).getMetadata().getAggregators();
        final File out = new File(work, "merged");
        final File mergedDir = new IndexMergerV9(m, indexIO).mergeQueryableIndex(indexes, false, metricAggs, out, indexSpec);

        final List<String> metricNames = new ArrayList<>();
        for (AggregatorFactory f : metricAggs == null ? new AggregatorFactory[0] : metricAggs) {
          metricNames.add(f.getName());
        }
        int rows = 0;
        for (QueryableIndex qi : indexes) {
          rows += qi.getNumRows();
        }
        final DataSegment template = new DataSegment(
            targetDataSource, merged, version, new LinkedHashMap<>(), dimensions, metricNames,
            LinearShardSpec.of(shard), SegmentUtils.getVersionFromDir(mergedDir), 0L, rows
        );
        final DataSegment pushed = DataSegmentPushers.s3(bucket, baseKey, disableAcl, accessKey, secretKey, endpoint, null)
                                                     .push(mergedDir, template);
        return m.writeValueAsString(pushed);
      }).collect();

      // 4) driver: bulk-publish all merged segments atomically
      final List<DataSegment> published = new ArrayList<>();
      for (String j : mergedJson) {
        published.add(mapper.readValue(j, DataSegment.class));
      }
      final int n = SegmentPublisher.publish(publishUrl, published, mapper);
      long bytes = 0;
      long rows = 0;
      for (DataSegment s : published) {
        bytes += s.getSize();
        rows += s.getNumRows();
      }
      System.out.println("druid-spark-compact: published " + n + " segment(s) for " + targetDataSource
                         + " (" + bytes + " bytes, " + rows + " rows) version " + version);
    }
    finally {
      spark.stop();
    }
  }

  private static IndexSpec toIndexSpec(ObjectMapper mapper, String secondaryIndexingJson) throws Exception
  {
    final JsonNode secondaryIndexing = mapper.readTree(secondaryIndexingJson);
    if (secondaryIndexing == null || secondaryIndexing.isNull() || secondaryIndexing.size() == 0) {
      return IndexSpec.DEFAULT;
    }
    final Map<String, SecondaryIndexingSpec> secondary = new LinkedHashMap<>();
    secondaryIndexing.fields().forEachRemaining(
        e -> secondary.put(e.getKey(), mapper.convertValue(e.getValue(), SecondaryIndexingSpec.class))
    );
    return new IndexSpec(null, null, null, null, secondary, null, false, null);
  }

  private static Interval span(Interval a, Interval b)
  {
    return new Interval(Math.min(a.getStartMillis(), b.getStartMillis()), Math.max(a.getEndMillis(), b.getEndMillis()));
  }

  private static String httpGet(String url) throws Exception
  {
    final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30_000);
    conn.setReadTimeout(300_000);
    try (InputStream in = conn.getInputStream()) {
      return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }
    finally {
      conn.disconnect();
    }
  }

  private SparkCompact() {}
}
