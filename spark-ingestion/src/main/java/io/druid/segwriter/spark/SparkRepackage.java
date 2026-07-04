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
import io.druid.storage.s3.S3SmooshDataSegmentPuller;
import io.druid.timeline.DataSegment;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.joda.time.Interval;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    // Accept either a single "dataSource" (string) or "dataSources" (array) — the scan runs per datasource.
    final List<String> dataSources = new ArrayList<>();
    if (spec.has("dataSources")) {
      for (JsonNode d : spec.get("dataSources")) {
        dataSources.add(d.asText());
      }
    } else {
      dataSources.add(spec.get("dataSource").asText());
    }
    final String bucket = spec.get("bucket").asText();            // source
    final String baseKey = spec.get("baseKey").asText();          // source
    // Optional relocation target. When destBucket/destBaseKey differ from the source, the job MOVES segments
    // (writes the s3_smoosh container to the new location, then — if deleteSource — deletes the whole source
    // prefix). Default = same location = in-place s3_zip->s3_smoosh transcode.
    final String destBucket = spec.has("destBucket") ? spec.get("destBucket").asText() : bucket;
    final String destBaseKey = spec.has("destBaseKey") ? spec.get("destBaseKey").asText() : baseKey;
    final boolean move = !destBucket.equals(bucket) || !destBaseKey.equals(baseKey);
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
      for (String dataSource : dataSources) {
        int found = 0;
        int skipped = 0;
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
              if (interval != null && !interval.contains(seg.getInterval())) {
                continue;
              }
              // In-place: only s3_zip needs transcoding (skip already-s3_smoosh -> idempotent). Move: relocate
              // both s3_zip and already-converted s3_smoosh out of the source location.
              final boolean convertible = "s3_zip".equals(type) || (move && "s3_smoosh".equals(type));
              if (convertible) {
                sources.add(mapper.writeValueAsString(seg));
                found++;
              } else if (!"s3_smoosh".equals(type)) {
                // null / other loadSpec (e.g. a broken/legacy descriptor): no source we can pull — skip.
                skipped++;
              }
            }
          }
          token = Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
        } while (token != null);
        System.out.println("druid-spark-repackage: " + dataSource + " -> " + found + " segment(s) to "
                           + (move ? "move" : "convert")
                           + (skipped > 0 ? " (" + skipped + " loadSpec-less skipped)" : ""));
      }

      if (sources.isEmpty()) {
        System.out.println("druid-spark-repackage: nothing to do in " + dataSources
                           + (move ? " (source already empty at " + bucket + "/" + baseKey + ")" : ""));
        return;
      }
      System.out.println("druid-spark-repackage: " + sources.size() + " segment(s) across " + dataSources.size()
                         + " datasource(s) -> s3_smoosh at " + destBucket + "/" + destBaseKey
                         + (move ? " (MOVE from " + bucket + "/" + baseKey + ")" : " (in place)")
                         + (deleteSource ? " + delete source" : ""));

      // 2) per-segment on executors: download+unzip -> s3_smoosh push (same prefix). No coordinator publish
      //    needed: a standalone historical picks it up from descriptor.json on its next scan. Each task catches
      //    its own failure and returns a status row ("OK\t<id>\t<bytes>" / "ERR\t<id>\t<msg>") so one bad segment
      //    can't abort the whole migration -- the already-converted ones are idempotently skipped on a re-run.
      final List<String> results = jsc.parallelize(sources, sources.size()).map(segJson -> {
        final ObjectMapper m = Json.indexMapper();
        final DataSegment source = m.readValue(segJson, DataSegment.class);
        try {
          final S3Client s3e = S3Clients.create(accessKey, secretKey, endpoint, null);
          final Map<String, Object> ls = source.getLoadSpec();
          final String type = String.valueOf(ls.get("type"));
          final long outBytes;

          if (move && "s3_smoosh".equals(type)) {
            // Relocate an already-s3_smoosh segment with SERVER-SIDE copies: the container objects go bucket->bucket
            // without streaming through the executor, so big segments can't OOM it (reconstruct+re-upload could).
            // Rewrite descriptor.json's loadSpec to the destination and skip the old index.zip.
            final String srcPrefix = String.valueOf(ls.get("prefix"));
            final String destPrefix = destBaseKey + srcPrefix.substring(baseKey.length());
            long total = 0;
            String token = null;
            do {
              final ListObjectsV2Request.Builder rb =
                  ListObjectsV2Request.builder().bucket(bucket).prefix(srcPrefix + "/");
              if (token != null) {
                rb.continuationToken(token);
              }
              final ListObjectsV2Response resp = s3e.listObjectsV2(rb.build());
              for (S3Object o : resp.contents()) {
                final String rel = o.key().substring(srcPrefix.length());   // "/header", "/00000.smoosh", ...
                if (rel.endsWith("/descriptor.json") || rel.endsWith("/index.zip")) {
                  continue;
                }
                s3e.copyObject(CopyObjectRequest.builder()
                                                .sourceBucket(bucket).sourceKey(o.key())
                                                .destinationBucket(destBucket).destinationKey(destPrefix + rel)
                                                .build());
                total += o.size();
              }
              token = Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
            } while (token != null);
            final Map<String, Object> destLs = new LinkedHashMap<>();
            destLs.put("type", "s3_smoosh");
            destLs.put("bucket", destBucket);
            destLs.put("prefix", destPrefix);
            final DataSegment out = source.withLoadSpec(destLs).withSize(total);
            putBytes(s3e, destBucket, destPrefix + "/descriptor.json", m.writeValueAsBytes(out));
            outBytes = total;
          } else {
            // Transcode: reconstruct a local v9 dir (s3_zip -> unzip; s3_smoosh -> rebuild) then write the
            // s3_smoosh container at the destination. Used for in-place s3_zip->s3_smoosh and any s3_zip move.
            final File dir = new File(Files.createTempDir(), "seg");
            if ("s3_smoosh".equals(type)) {
              new S3SmooshDataSegmentPuller(s3e).getSegmentFiles(source, dir);
            } else {
              new S3DataSegmentPuller(s3e).getSegmentFiles(source, dir);   // s3_zip
            }
            outBytes = DataSegmentPushers
                .s3Smoosh(destBucket, destBaseKey, disableAcl, accessKey, secretKey, endpoint, null)
                .push(dir, source).getSize();
          }

          if (deleteSource) {
            if (move) {
              // true move: dest is a different location, so drop the ENTIRE source prefix (index.zip + header +
              // chunks + descriptor). Derive the source segment dir from its loadSpec.
              final String srcPrefix;
              if ("s3_smoosh".equals(type)) {
                srcPrefix = String.valueOf(ls.get("prefix"));
              } else {
                final String key = String.valueOf(ls.get("key"));   // s3_zip: <prefix>/index.zip
                srcPrefix = key.substring(0, key.lastIndexOf('/'));
              }
              deletePrefix(s3e, bucket, srcPrefix);
            } else if ("s3_zip".equals(type) && ls.get("key") != null) {
              // in place: only the now-orphaned index.zip
              s3e.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(String.valueOf(ls.get("key"))).build());
            }
          }
          return "OK\t" + source.getIdentifier() + "\t" + outBytes;
        }
        catch (Throwable t) {
          final StringBuilder trace = new StringBuilder(String.valueOf(t));
          final StackTraceElement[] st = t.getStackTrace();
          for (int i = 0; i < Math.min(6, st.length); i++) {
            trace.append(" | ").append(st[i]);
          }
          return "ERR\t" + source.getIdentifier() + "\t" + trace;
        }
      }).collect();

      long bytes = 0;
      int ok = 0;
      final List<String> failures = new ArrayList<>();
      for (String r : results) {
        final String[] p = r.split("\t", 3);
        if ("OK".equals(p[0])) {
          ok++;
          bytes += Long.parseLong(p[2]);
        } else {
          failures.add(p.length > 1 ? p[1] + " -> " + (p.length > 2 ? p[2] : "?") : r);
        }
      }
      System.out.println("druid-spark-repackage: converted " + ok + "/" + results.size() + " segment(s) across "
                         + dataSources.size() + " datasource(s) to s3_smoosh (" + bytes + " bytes), "
                         + failures.size() + " failed");
      for (String f : failures) {
        System.out.println("druid-spark-repackage: FAILED " + f);
      }
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

  private static void putBytes(S3Client s3, String bucket, String key, byte[] bytes)
  {
    s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromBytes(bytes));
  }

  /** Delete every object under a segment's source prefix (header + chunks + descriptor.json + index.zip). */
  private static void deletePrefix(S3Client s3, String bucket, String prefix)
  {
    final String p = prefix.endsWith("/") ? prefix : prefix + "/";
    String token = null;
    do {
      final ListObjectsV2Request.Builder rb = ListObjectsV2Request.builder().bucket(bucket).prefix(p);
      if (token != null) {
        rb.continuationToken(token);
      }
      final ListObjectsV2Response resp = s3.listObjectsV2(rb.build());
      for (S3Object o : resp.contents()) {
        s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(o.key()).build());
      }
      token = Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
    } while (token != null);
  }

  private SparkRepackage()
  {
  }
}
