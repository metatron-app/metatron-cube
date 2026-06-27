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

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import io.druid.java.util.common.logger.Logger;
import io.druid.tasklogs.TaskLogs;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

/**
 * Provides task logs archived on S3.
 */
public class S3TaskLogs implements TaskLogs
{
  private static final Logger log = new Logger(S3TaskLogs.class);

  private final S3Client service;
  private final S3TaskLogsConfig config;

  @Inject
  public S3TaskLogs(S3TaskLogsConfig config, S3Client service)
  {
    this.config = config;
    this.service = service;
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid);

    try {
      final HeadObjectResponse objectDetails = service.headObject(
          HeadObjectRequest.builder().bucket(config.getS3Bucket()).key(taskKey).build()
      );

      final long length = objectDetails.contentLength();
      final String etag = objectDetails.eTag();

      return Optional.<ByteSource>of(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              try {
                final long start;
                final long end = length - 1;

                if (offset > 0 && offset < length) {
                  start = offset;
                } else if (offset < 0 && (-1 * offset) < length) {
                  start = length + offset;
                } else {
                  start = 0;
                }

                return service.getObject(
                    GetObjectRequest.builder()
                        .bucket(config.getS3Bucket())
                        .key(taskKey)
                        .ifMatch(etag)
                        .range(String.format("bytes=%d-%d", start, end))
                        .build()
                );
              }
              catch (S3Exception e) {
                throw new IOException(e);
              }
            }
          }
      );
    }
    catch (S3Exception e) {
      final String errorCode = e.awsErrorDetails() == null ? null : e.awsErrorDetails().errorCode();
      if (404 == e.statusCode()
          || "NoSuchKey".equals(errorCode)
          || "NoSuchBucket".equals(errorCode)) {
        return Optional.absent();
      } else {
        throw new IOException(String.format("Failed to stream logs from: %s", taskKey), e);
      }
    }
  }

  @Override
  public void pushTaskLog(final String taskid, final File logFile)
  {
    final String taskKey = getTaskLogKey(taskid);
    log.info("Pushing task log %s to: %s", logFile, taskKey);

    try {
      S3Utils.retryS3Operation(
          new Callable<Void>()
          {
            @Override
            public Void call() throws Exception
            {
              service.putObject(
                  PutObjectRequest.builder().bucket(config.getS3Bucket()).key(taskKey).build(),
                  RequestBody.fromFile(logFile.toPath())
              );
              return null;
            }
          }
      );
    }
    catch (Throwable t) {
      log.warn(t, "Failed to write task log to: %s", config.getS3Bucket());
    }
  }

  private String getTaskLogKey(String taskid)
  {
    return String.format("%s/%s/log", config.getS3Prefix(), taskid);
  }

  @Override
  public void killAll() throws IOException
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void killOlderThan(long timestamp) throws IOException
  {
    throw new UnsupportedOperationException("not implemented");
  }
}
