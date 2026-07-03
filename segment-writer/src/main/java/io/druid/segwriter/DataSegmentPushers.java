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

import io.druid.segment.loading.DataSegmentPusher;
import io.druid.storage.s3.S3Clients;
import io.druid.storage.s3.S3DataSegmentPusher;
import io.druid.storage.s3.S3DataSegmentPusherConfig;
import io.druid.storage.s3.S3SmooshDataSegmentPusher;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;

/**
 * Factories for the {@link DataSegmentPusher}s the writer can use, built without Guice.
 */
public final class DataSegmentPushers
{
  private DataSegmentPushers() {}

  /**
   * aws-sdk v2 S3 pusher (works against AWS or an S3-compatible gateway like SeaweedFS).
   *
   * @param accessKey static key, or null/empty to use the default credential chain
   *                  (env AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, profile, role, ...)
   * @param endpoint  endpoint override (e.g. http://seaweed.s2dev.net:8333); empty => real AWS.
   *                  A non-empty endpoint enables path-style addressing.
   */
  public static DataSegmentPusher s3(
      String bucket,
      String baseKey,
      boolean disableAcl,
      String accessKey,
      String secretKey,
      String endpoint,
      String region
  )
  {
    final S3Client client = S3Clients.create(accessKey, secretKey, endpoint, region);
    final S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket(bucket);
    config.setBaseKey(baseKey);
    config.setDisableAcl(disableAcl);
    return new S3DataSegmentPusher(client, config, Json.mapper());
  }

  /**
   * aws-sdk v2 pusher for the unzipped {@code s3_smoosh} container (separate header + chunk objects, no zip).
   * Same params/credentials as {@link #s3}; differs only in the deep-storage layout it writes.
   */
  public static DataSegmentPusher s3Smoosh(
      String bucket,
      String baseKey,
      boolean disableAcl,
      String accessKey,
      String secretKey,
      String endpoint,
      String region
  )
  {
    final S3Client client = S3Clients.create(accessKey, secretKey, endpoint, region);
    final S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket(bucket);
    config.setBaseKey(baseKey);
    config.setDisableAcl(disableAcl);
    // indexMapper (not mapper): ContainerHeader.write deserializes each column's descriptor to read its
    // capabilities, so the extension column part serdes (e.g. "lucene10") must be resolvable.
    return new S3SmooshDataSegmentPusher(client, config, Json.indexMapper());
  }

  /** Local-filesystem pusher (tests / local runs). */
  public static DataSegmentPusher local(File baseDir)
  {
    return new LocalDataSegmentPusher(baseDir);
  }
}
