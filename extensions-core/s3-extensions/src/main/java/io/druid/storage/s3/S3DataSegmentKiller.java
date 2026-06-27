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

import com.google.inject.Inject;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.util.Map;

/**
 */
public class S3DataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(S3DataSegmentKiller.class);

  private final S3Client s3Client;

  @Inject
  public S3DataSegmentKiller(
      S3Client s3Client
  )
  {
    this.s3Client = s3Client;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, "bucket");
      String s3Path = MapUtils.getString(loadSpec, "key");
      String s3DescriptorPath = S3Utils.descriptorPathForSegmentPath(s3Path);

      if (isObjectInBucket(s3Bucket, s3Path)) {
        log.info("Removing index file[s3://%s/%s] from s3!", s3Bucket, s3Path);
        s3Client.deleteObject(DeleteObjectRequest.builder().bucket(s3Bucket).key(s3Path).build());
      }
      if (isObjectInBucket(s3Bucket, s3DescriptorPath)) {
        log.info("Removing descriptor file[s3://%s/%s] from s3!", s3Bucket, s3DescriptorPath);
        s3Client.deleteObject(DeleteObjectRequest.builder().bucket(s3Bucket).key(s3DescriptorPath).build());
      }
    }
    catch (SdkException e) {
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]: [%s]", segment.getIdentifier(), e);
    }
  }

  private boolean isObjectInBucket(String bucket, String key)
  {
    try {
      s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
      return true;
    }
    catch (NoSuchKeyException e) {
      return false;
    }
  }

  @Override
  public void killAll() throws IOException
  {
    throw new UnsupportedOperationException("not implemented");
  }
}
