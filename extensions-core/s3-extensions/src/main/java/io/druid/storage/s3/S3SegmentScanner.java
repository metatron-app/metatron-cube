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
    final List<DataSegment> segments = new ArrayList<>();
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
            segments.add(jsonMapper.readValue(getObject(config.getBucket(), o.key()), DataSegment.class));
          }
        }
        token = Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
      } while (token != null);
    }
    log.info("standalone S3 scan: %d descriptor(s) under %d datasource(s) in bucket[%s]",
             segments.size(), config.getDataSources().size(), config.getBucket());
    return segments;
  }

  private byte[] getObject(String bucket, String key) throws IOException
  {
    try (ResponseInputStream<GetObjectResponse> in =
             s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())) {
      return ByteStreams.toByteArray(in);
    }
  }
}
