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
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.druid.segwriter.DataSegmentPushers;
import io.druid.segwriter.Json;
import io.druid.storage.s3.S3Clients;
import io.druid.storage.s3.S3DataSegmentPuller;
import io.druid.timeline.DataSegment;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.joda.time.Interval;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Convert existing zipped {@code s3_zip} segments to the unzipped, range-readable {@code s3_smoosh} container
 * IN PLACE, without re-indexing. The columnar payload (the {@code NNNNN.smoosh} chunks incl. dictionaries and
 * lucene indexes) is byte-identical between the two containers — only the wrapper (a single {@code index.zip}
 * DEFLATE blob vs. separate chunk objects + a readable header) and the {@code descriptor.json} loadSpec differ.
 * So this is a pure I/O transcode (download + unzip + re-upload), NOT a re-index — which also means it works for
 * index-only lucene columns that can't be rebuilt (their source text is gone).
 *
 * <p>Per segment on an executor: {@link S3DataSegmentPuller#getSegmentFiles} fetches+unzips {@code index.zip} into
 * a temp dir, then {@link DataSegmentPushers#s3Smoosh} writes the header + raw {@code NNNNN.smoosh} objects and
 * overwrites {@code descriptor.json} with an {@code s3_smoosh} loadSpec — at the SAME prefix (the pusher derives
 * the prefix from segment identity). The old {@code index.zip} is thereby orphaned; set {@code deleteSource:true}
 * to remove it in the same pass (safe: the pusher writes descriptor.json LAST, so the flip is atomic per segment).
 *
 * <p>Source segments are found by a deep-storage descriptor scan (no coordinator / metadata DB) — the same list
 * a {@code standalone} historical would load; already-{@code s3_smoosh} segments are skipped so re-runs are no-ops.
 *
 * <p>Spec JSON: {@code {dataSource, bucket, baseKey, endpoint, disableAcl?, interval?, deleteSource?}}.
 */
public final class SparkRepackage
{
  public static void main(String[] args) throws Exception
  {
    final ObjectMapper mapper = Json.indexMapper();
    final JsonNode spec = mapper.readTree(new File(args[0]));

    final String dataSource = spec.get("dataSource").asText();
    final String bucket = spec.get("bucket").asText();
    final String baseKey = spec.get("baseKey").asText();
    final String endpoint = spec.get("endpoint").asText();
    final boolean disableAcl = spec.path("disableAcl").asBoolean(true);
    final boolean deleteSource = spec.path("deleteSource").asBoolean(false);
    final Interval interval = spec.has("interval") ? new Interval(spec.get("interval").asText()) : null;
    final String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    final String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");

    final SparkSession spark = SparkSession.builder().appName("druid-segment-repackage").getOrCreate();
    try (JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
      // 1) driver: deep-storage descriptor scan -> source s3_zip segments (skip already-converted s3_smoosh)
      final S3Client s3 = S3Clients.create(accessKey, secretKey, endpoint, null);
      final List<String> sources = new ArrayList<>();
      final String prefix = baseKey + "/" + dataSource + "/";
      String token = null;
      do {
        final ListObjectsV2Request.Builder rb = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix);
        if (token != null) {
          rb.continuationToken(token);
        }
        final ListObjectsV2Response resp = s3.listObjectsV2(rb.build());
        for (S3Object o : resp.contents()) {
          if (o.key().endsWith("/descriptor.json")) {
            final DataSegment seg = mapper.readValue(getObject(s3, bucket, o.key()), DataSegment.class);
            final Object type = seg.getLoadSpec() == null ? null : seg.getLoadSpec().get("type");
            if (!"s3_smoosh".equals(type) && (interval == null || interval.contains(seg.getInterval()))) {
              sources.add(mapper.writeValueAsString(seg));
            }
          }
        }
        token = Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
      } while (token != null);

      if (sources.isEmpty()) {
        System.out.println("druid-spark-repackage: no non-s3_smoosh segment for " + dataSource + " -> nothing to do");
        return;
      }
      System.out.println("druid-spark-repackage: " + sources.size() + " segment(s) of " + dataSource
                         + " -> s3_smoosh" + (deleteSource ? " (deleting source index.zip)" : ""));

      // 2) per-segment on executors: download+unzip -> s3_smoosh push (same prefix). No coordinator publish
      //    needed: a standalone historical picks it up from descriptor.json on its next scan.
      final List<String> done = jsc.parallelize(sources, sources.size()).map(segJson -> {
        final ObjectMapper m = Json.indexMapper();
        final S3Client s3e = S3Clients.create(accessKey, secretKey, endpoint, null);
        final S3DataSegmentPuller puller = new S3DataSegmentPuller(s3e);
        final DataSegment source = m.readValue(segJson, DataSegment.class);

        final File dir = new File(Files.createTempDir(), "seg");
        puller.getSegmentFiles(source, dir);   // fetch + unzip index.zip

        final DataSegment out = DataSegmentPushers
            .s3Smoosh(bucket, baseKey, disableAcl, accessKey, secretKey, endpoint, null)
            .push(dir, source);

        if (deleteSource && source.getLoadSpec() != null) {
          final Object zipKey = source.getLoadSpec().get("key");   // s3_zip loadSpec: {type,bucket,key=.../index.zip}
          if (zipKey != null) {
            s3e.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(String.valueOf(zipKey)).build());
          }
        }
        return m.writeValueAsString(out);
      }).collect();

      long bytes = 0;
      for (String j : done) {
        bytes += mapper.readValue(j, DataSegment.class).getSize();
      }
      System.out.println("druid-spark-repackage: converted " + done.size() + " segment(s) of " + dataSource
                         + " to s3_smoosh (" + bytes + " bytes)");
    }
    finally {
      spark.stop();
    }
  }

  private static byte[] getObject(S3Client s3, String bucket, String key) throws Exception
  {
    try (ResponseInputStream<GetObjectResponse> in =
             s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())) {
      return ByteStreams.toByteArray(in);
    }
  }

  private SparkRepackage()
  {
  }
}
