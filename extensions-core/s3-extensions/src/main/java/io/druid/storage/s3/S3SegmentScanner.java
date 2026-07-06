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

package io.druid.storage.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.SegmentScanner;
import io.druid.timeline.DataSegment;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Deep-storage (S3) {@link SegmentScanner} for the standalone historical: lists {@code <baseKey>/<datasource>/}
 * and parses every {@code descriptor.json} into a {@link DataSegment} — no coordinator / metadata DB needed.
 */
public class S3SegmentScanner implements SegmentScanner
{
  private static final Logger log = new Logger(S3SegmentScanner.class);

  private final S3Client s3Client;
  private final ObjectMapper jsonMapper;
  private final S3StandaloneScanConfig config;

  @Inject
  public S3SegmentScanner(S3Client s3Client, @Json ObjectMapper jsonMapper, S3StandaloneScanConfig config)
  {
    this.s3Client = s3Client;
    this.jsonMapper = jsonMapper;
    this.config = config;
  }

  @Override
  public List<DataSegment> scan() throws IOException
  {
    // 1) list descriptor keys (cheap: 1000/page)
    final List<String> keys = new ArrayList<>();
    for (String dataSource : config.getDataSources()) {
      final String prefix = config.getBaseKey() + "/" + dataSource + "/";
      String token = null;
      do {
        final ListObjectsV2Request.Builder rb =
            ListObjectsV2Request.builder().bucket(config.getBucket()).prefix(prefix);
        if (token != null) {
          rb.continuationToken(token);
        }
        final ListObjectsV2Response resp = s3Client.listObjectsV2(rb.build());
        for (S3Object o : resp.contents()) {
          if (o.key().endsWith("/descriptor.json")) {
            keys.add(o.key());
          }
        }
        token = Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
      } while (token != null);
    }

    // 2) fetch+parse descriptors in parallel — one GET per segment done sequentially makes a large datasource
    //    (tens of thousands of segments) take minutes to scan at boot; a bounded pool cuts that ~Nx.
    final long start = System.currentTimeMillis();
    final int total = keys.size();
    final int threads = Math.min(64, Math.max(8, total / 200));
    log.info("standalone S3 scan: listed %d descriptor(s) in bucket[%s], fetching via %d threads...",
             total, config.getBucket(), threads);
    final ExecutorService pool = Executors.newFixedThreadPool(threads, daemonFactory());
    final List<DataSegment> segments = new ArrayList<>(total);
    final int step = Math.max(1, total / 10);   // progress log ~every 10%
    try {
      final List<Future<DataSegment>> futures = new ArrayList<>(total);
      for (String key : keys) {
        futures.add(pool.submit(() -> jsonMapper.readValue(getObject(config.getBucket(), key), DataSegment.class)));
      }
      int done = 0;
      for (Future<DataSegment> f : futures) {
        segments.add(f.get());
        if (++done % step == 0 || done == total) {
          log.info("standalone S3 scan: fetched %d%% (%d/%d) in %dms",
                   100 * done / total, done, total, System.currentTimeMillis() - start);
        }
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
    finally {
      pool.shutdown();
    }
    log.info("standalone S3 scan: %d descriptor(s) under %d datasource(s) in bucket[%s] via %d threads in %dms",
             segments.size(), config.getDataSources().size(), config.getBucket(), threads,
             System.currentTimeMillis() - start);
    return segments;
  }

  private static ThreadFactory daemonFactory()
  {
    final AtomicInteger n = new AtomicInteger();
    return r -> {
      final Thread t = new Thread(r, "segment-scan-" + n.getAndIncrement());
      t.setDaemon(true);
      return t;
    };
  }

  private byte[] getObject(String bucket, String key) throws IOException
  {
    try (ResponseInputStream<GetObjectResponse> in =
             s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())) {
      return ByteStreams.toByteArray(in);
    }
  }
}
