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

import com.google.common.base.Strings;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

/**
 * Builds an aws-sdk v2 {@link S3Client} the same way the Druid module does, but with no Guice
 * dependency so it can be used from a standalone/embedded context (e.g. a Spark segment writer).
 */
public class S3Clients
{
  // Bounded concurrent connections per client. Caps sockets well below the ephemeral-port ceiling while giving the
  // 32-way range-fetch parallelism ample headroom; excess demand QUEUES on the pool rather than opening new sockets.
  private static final int MAX_CONCURRENCY = 128;


  /**
   * @param accessKey static access key, or null/empty to use the default credential chain
   *                  (env AWS_ACCESS_KEY_ID, profile, instance role, ...)
   * @param secretKey static secret key (paired with accessKey)
   * @param endpoint  S3-compatible endpoint override (e.g. SeaweedFS/MinIO); empty => real AWS.
   *                  A non-empty endpoint enables path-style addressing.
   * @param region    AWS region; empty => us-east-1 (gateways usually ignore it)
   */
  public static S3Client create(String accessKey, String secretKey, String endpoint, String region)
  {
    final AwsCredentialsProvider credentials;
    if (!Strings.isNullOrEmpty(accessKey) && !Strings.isNullOrEmpty(secretKey)) {
      credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
    } else {
      credentials = DefaultCredentialsProvider.create();
    }

    final S3ClientBuilder builder = S3Client.builder()
        .credentialsProvider(credentials)
        // CRT HTTP client: a real bounded connection pool (reuses + caps sockets) so the range-fetch GET storm
        // can't exhaust ephemeral ports. Self-contained native networking => no classpath conflict with the
        // project's legacy apache httpclient (which is why aws-sdk v2's ApacheHttpClient was avoided).
        .httpClientBuilder(AwsCrtHttpClient.builder().maxConcurrency(MAX_CONCURRENCY));

    builder.region(!Strings.isNullOrEmpty(region) ? Region.of(region) : Region.US_EAST_1);

    if (!Strings.isNullOrEmpty(endpoint)) {
      builder.endpointOverride(URI.create(endpoint)).forcePathStyle(true);
    }
    return builder.build();
  }
}
