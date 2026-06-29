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
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.loading.DataSegmentPusher;
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
    final ObjectMapper mapper = new DefaultObjectMapper();
    final String specJson = new String(
        java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(args[0])),
        java.nio.charset.StandardCharsets.UTF_8
    );
    final SegmentIngestSpec spec = mapper.readValue(specJson, SegmentIngestSpec.class);
    final int numShards = spec.getNumShards();
    final String version = new DateTime().toString();

    final SparkSession spark = SparkSession.builder().appName("druid-segment-ingestion").getOrCreate();
    try {
      final Dataset<Row> df = spark.read().format(spec.getFormat())
                                   .load(spec.getPaths().toArray(new String[0]));

      // Row -> (key=(intervalStart, shard), event map)
      final JavaPairRDD<Tuple2<Long, Integer>, Map<String, Object>> keyed =
          df.toJavaRDD().mapToPair(row -> {
            final SegmentIngestSpec s = new DefaultObjectMapper().readValue(specJson, SegmentIngestSpec.class);
            final Map<String, Object> event = rowToMap(row);
            final long ts = toMillis(event.get(s.getTimestampColumn()));
            event.put(s.getTimestampColumn(), ts);   // normalize to epoch millis for the writer
            final Interval iv = SegmentIngestor.bucket(s, ts);
            final int shard = s.getNumShards() <= 1 ? 0 : Math.floorMod(shardKey(event, s), s.getNumShards());
            return new Tuple2<>(new Tuple2<>(iv.getStartMillis(), shard), event);
          });

      // one segment per (interval, shard)
      final JavaRDD<String> segmentJsons = keyed.groupByKey().map(entry -> {
        final ObjectMapper m = new DefaultObjectMapper();
        final SegmentIngestSpec s = m.readValue(specJson, SegmentIngestSpec.class);
        final long bucketStart = entry._1()._1();
        final int shard = entry._1()._2();
        final Interval iv = SegmentIngestor.bucket(s, bucketStart);
        final DataSegmentPusher pusher = SegmentIngestor.pusher(s);
        final File tmp = Files.createTempDir();
        final DataSegment seg = SegmentIngestor.buildSegment(
            s, iv, version, shard, s.getNumShards(), entry._2().iterator(), tmp, pusher
        );
        return m.writeValueAsString(seg);
      });

      final List<String> built = segmentJsons.collect();

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
