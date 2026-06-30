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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segwriter.Json;
import io.druid.segwriter.SegmentIngestSpec;
import io.druid.segwriter.SegmentIngestor;
import io.druid.segwriter.SegmentPublisher;
import io.druid.timeline.DataSegment;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spec-driven Spark ingestion: read source rows, build one Druid segment per (interval, shard),
 * push to deep storage, and publish via the overlord. No MapReduce, no Hadoop, no Druid task.
 *
 *   spark-submit --class io.druid.segwriter.spark.SparkIngestion \
 *       druid-spark-ingestion-<ver>-spark.jar  /path/to/spec.json
 *
 * The spec (see SegmentIngestSpec) is parsed with Druid's mapper, so "metrics" use native
 * aggregator JSON. S3 credentials come from AWS_ env on the executors (k8s secret).
 */
public final class SparkIngestion
{
  public static void main(String[] args) throws Exception
  {
    if (args.length < 1) {
      System.err.println("usage: SparkIngestion <spec.json path>");
      System.exit(2);
    }
    final ObjectMapper mapper = Json.mapper();
    final String specJson = new String(
        java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(args[0])),
        java.nio.charset.StandardCharsets.UTF_8
    );
    final SegmentIngestSpec spec = mapper.readValue(specJson, SegmentIngestSpec.class);
    final int numShards = spec.getNumShards();
    final String version = new DateTime().toString();

    final SparkSession.Builder builder = SparkSession.builder().appName("druid-segment-ingestion");
    // Inject the Polaris OAuth2 client credential into the iceberg catalog from env, so it never lives
    // in the (declarative) sparkConf/YAML. The rest of the catalog config (uri, warehouse, realm header,
    // io-impl) stays in sparkConf; Polaris itself vends the S3 endpoint + keys via its config response.
    final String icebergCatalog = System.getenv().getOrDefault("ICEBERG_CATALOG", "iceberg");
    final String polarisId = System.getenv("POLARIS_CLIENT_ID");
    final String polarisSecret = System.getenv("POLARIS_CLIENT_SECRET");
    if (polarisId != null && polarisSecret != null) {
      builder.config("spark.sql.catalog." + icebergCatalog + ".credential", polarisId + ":" + polarisSecret);
    }
    // The source S3 store (where the iceberg data files live) can differ from the deep-storage S3 used
    // to push segments (the AWS_* env). When the catalog doesn't vend creds, set its S3FileIO creds here
    // from dedicated env, keeping them separate from AWS_* (deep storage).
    final String s3Id = System.getenv("ICEBERG_S3_ACCESS_KEY_ID");
    final String s3Secret = System.getenv("ICEBERG_S3_SECRET_ACCESS_KEY");
    if (s3Id != null && s3Secret != null) {
      builder.config("spark.sql.catalog." + icebergCatalog + ".s3.access-key-id", s3Id);
      builder.config("spark.sql.catalog." + icebergCatalog + ".s3.secret-access-key", s3Secret);
    }
    final SparkSession spark = builder.getOrCreate();
    try {
      // read the configured source (file paths or an iceberg table via the Polaris catalog)
      final io.druid.segwriter.SourceSpec source = spec.getSource();
      Dataset<Row> df;
      if (source instanceof io.druid.segwriter.IcebergSource) {
        df = spark.table(((io.druid.segwriter.IcebergSource) source).getTable());
      } else if (source instanceof io.druid.segwriter.FileSource) {
        final io.druid.segwriter.FileSource fs = (io.druid.segwriter.FileSource) source;
        df = spark.read().format(fs.getFormat()).load(fs.getPaths().toArray(new String[0]));
      } else {
        throw new IllegalArgumentException("spec.source is required (type \"file\" or \"iceberg\"); got: " + source);
      }
      if (source.getWhere() != null && !source.getWhere().isEmpty()) {
        df = df.where(source.getWhere());   // optional bound; iceberg prunes partitions
      }

      final List<String> built;
      if ("aligned".equalsIgnoreCase(spec.getLayout())) {
        // shuffle-free: each Spark input partition is already one segment's worth of rows (the source
        // is partitioned on the timestamp column and segmentGranularity matches). Build one segment per
        // partition with mapPartitions — no groupByKey, no in-memory group buffering. The writer streams
        // the partition iterator straight into the index. Shard = the (globally unique) partition id.
        built = df.toJavaRDD().mapPartitions(rowIter -> {
          if (!rowIter.hasNext()) {
            return java.util.Collections.<String>emptyIterator();
          }
          final ObjectMapper m = Json.mapper();
          final SegmentIngestSpec s = m.readValue(specJson, SegmentIngestSpec.class);
          final int shard = org.apache.spark.TaskContext.getPartitionId();
          // Stream the partition straight into one segment (no row buffering): the writer iterates once
          // and only the IncrementalIndex holds rows. Assumes one interval per partition — true when the
          // source partition granularity == segmentGranularity (e.g. hour(ts) table + HOUR segments).
          final java.util.Iterator<Map<String, Object>> events =
              com.google.common.collect.Iterators.transform(rowIter, row -> {
                final Map<String, Object> e = rowToMap(row);
                e.put(s.getTimestampColumn(), toMillis(e.get(s.getTimestampColumn())));   // -> epoch millis
                return e;
              });
          final com.google.common.collect.PeekingIterator<Map<String, Object>> rows =
              com.google.common.collect.Iterators.peekingIterator(events);
          final Interval iv = SegmentIngestor.bucket(s, ((Number) rows.peek().get(s.getTimestampColumn())).longValue());
          final DataSegmentPusher pusher = SegmentIngestor.pusher(s);
          final File tmp = Files.createTempDir();
          final DataSegment seg = SegmentIngestor.buildSegment(
              s, iv, version, shard, Integer.MAX_VALUE, rows, tmp, pusher   // numShards>1 -> LinearShardSpec(shard)
          );
          return java.util.Collections.singletonList(m.writeValueAsString(seg)).iterator();
        }).collect();
      } else {
        // keyed (default): shuffle rows into (interval,shard) groups, one segment per group
        final JavaPairRDD<Tuple2<Long, Integer>, Map<String, Object>> keyed =
            df.toJavaRDD().mapToPair(row -> {
              final SegmentIngestSpec s = Json.mapper().readValue(specJson, SegmentIngestSpec.class);
              final Map<String, Object> event = rowToMap(row);
              final long ts = toMillis(event.get(s.getTimestampColumn()));
              event.put(s.getTimestampColumn(), ts);   // normalize to epoch millis for the writer
              final Interval iv = SegmentIngestor.bucket(s, ts);
              final int shard = s.getNumShards() <= 1 ? 0 : Math.floorMod(shardKey(event, s), s.getNumShards());
              return new Tuple2<>(new Tuple2<>(iv.getStartMillis(), shard), event);
            });

        built = keyed.groupByKey().map(entry -> {
          final ObjectMapper m = Json.mapper();
          final SegmentIngestSpec s = m.readValue(specJson, SegmentIngestSpec.class);
          final Interval iv = SegmentIngestor.bucket(s, entry._1()._1());
          final DataSegmentPusher pusher = SegmentIngestor.pusher(s);
          final File tmp = Files.createTempDir();
          final DataSegment seg = SegmentIngestor.buildSegment(
              s, iv, version, entry._1()._2(), s.getNumShards(), entry._2().iterator(), tmp, pusher
          );
          return m.writeValueAsString(seg);
        }).collect();
      }

      // driver publishes the whole batch atomically via the overlord
      final List<DataSegment> segments = new ArrayList<>(built.size());
      for (String j : built) {
        segments.add(mapper.readValue(j, DataSegment.class));
      }
      final int published = SegmentPublisher.publish(spec.getPublishUrl(), segments, mapper);
      System.out.println("druid-spark-ingestion: built " + segments.size() + " segment(s), published " + published);
    }
    finally {
      spark.stop();
    }
  }

  private static Map<String, Object> rowToMap(Row row)
  {
    final Map<String, Object> map = new HashMap<>();
    final String[] names = row.schema().fieldNames();
    for (int i = 0; i < names.length; i++) {
      map.put(names[i], row.isNullAt(i) ? null : row.get(i));
    }
    return map;
  }

  private static int shardKey(Map<String, Object> event, SegmentIngestSpec spec)
  {
    int h = 1;
    for (String d : spec.getDimensions()) {
      final Object v = event.get(d);
      h = 31 * h + (v == null ? 0 : v.hashCode());
    }
    return h;
  }

  private static long toMillis(Object ts)
  {
    if (ts instanceof Number) {
      return ((Number) ts).longValue();
    }
    if (ts instanceof java.sql.Timestamp) {
      return ((java.sql.Timestamp) ts).getTime();
    }
    if (ts instanceof java.util.Date) {
      return ((java.util.Date) ts).getTime();
    }
    if (ts instanceof java.time.Instant) {
      return ((java.time.Instant) ts).toEpochMilli();
    }
    if (ts instanceof CharSequence) {
      return DateTime.parse(ts.toString()).getMillis();
    }
    throw new IllegalArgumentException("unsupported timestamp value: " + ts);
  }
}
