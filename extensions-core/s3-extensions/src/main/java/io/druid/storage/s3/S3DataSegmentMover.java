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

import com.google.common.base.Predicate;
import io.druid.java.util.common.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class S3DataSegmentMover implements DataSegmentMover
{
  private static final Logger log = new Logger(S3DataSegmentMover.class);

  private static final String STORAGE_CLASS_GLACIER = "GLACIER";

  private final S3Client s3Client;
  private final S3DataSegmentPusherConfig config;

  @Inject
  public S3DataSegmentMover(
      S3Client s3Client,
      S3DataSegmentPusherConfig config
  )
  {
    this.s3Client = s3Client;
    this.config = config;
  }

  @Override
  public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, "bucket");
      String s3Path = MapUtils.getString(loadSpec, "key");
      String s3DescriptorPath = S3Utils.descriptorPathForSegmentPath(s3Path);

      final String targetS3Bucket = MapUtils.getString(targetLoadSpec, "bucket");
      final String targetS3BaseKey = MapUtils.getString(targetLoadSpec, "baseKey");

      final String targetS3Path = S3Utils.constructSegmentPath(targetS3BaseKey, segment);
      String targetS3DescriptorPath = S3Utils.descriptorPathForSegmentPath(targetS3Path);

      if (targetS3Bucket.isEmpty()) {
        throw new SegmentLoadingException("Target S3 bucket is not specified");
      }
      if (targetS3Path.isEmpty()) {
        throw new SegmentLoadingException("Target S3 baseKey is not specified");
      }

      safeMove(s3Bucket, s3Path, targetS3Bucket, targetS3Path);
      safeMove(s3Bucket, s3DescriptorPath, targetS3Bucket, targetS3DescriptorPath);

      return segment.withLoadSpec(
          ImmutableMap.<String, Object>builder()
              .putAll(
                  Maps.filterKeys(
                      loadSpec, new Predicate<String>()
                      {
                        @Override
                        public boolean apply(String input)
                        {
                          return !(input.equals("bucket") || input.equals("key"));
                        }
                      }
                  )
              )
              .put("bucket", targetS3Bucket)
              .put("key", targetS3Path)
              .build()
      );
    }
    catch (SdkException e) {
      throw new SegmentLoadingException(e, "Unable to move segment[%s]: [%s]", segment.getIdentifier(), e);
    }
  }

  private void safeMove(
      final String s3Bucket,
      final String s3Path,
      final String targetS3Bucket,
      final String targetS3Path
  ) throws SdkException, SegmentLoadingException
  {
    try {
      S3Utils.retryS3Operation(
          new Callable<Void>()
          {
            @Override
            public Void call() throws Exception
            {
              if (s3Bucket.equals(targetS3Bucket) && s3Path.equals(targetS3Path)) {
                log.info("No need to move file[s3://%s/%s] onto itself", s3Bucket, s3Path);
                return null;
              }
              if (isObjectInBucket(s3Bucket, s3Path)) {
                final ListObjectsV2Response listing = s3Client.listObjectsV2(
                    ListObjectsV2Request.builder().bucket(s3Bucket).prefix(s3Path).build()
                );
                final List<S3Object> objects = listing.contents();
                if (objects.isEmpty()) {
                  // should never happen
                  throw new ISE("Unable to list object [s3://%s/%s]", s3Bucket, s3Path);
                }
                final S3Object s3Object = objects.get(0);
                if (STORAGE_CLASS_GLACIER.equals(s3Object.storageClassAsString())) {
                  log.warn("Cannot move file[s3://%s/%s] of storage class glacier, skipping.", s3Bucket, s3Path);
                } else {
                  log.info(
                      "Moving file[s3://%s/%s] to [s3://%s/%s]",
                      s3Bucket,
                      s3Path,
                      targetS3Bucket,
                      targetS3Path
                  );
                  final CopyObjectRequest.Builder copyRequest = CopyObjectRequest.builder()
                      .sourceBucket(s3Bucket)
                      .sourceKey(s3Path)
                      .destinationBucket(targetS3Bucket)
                      .destinationKey(targetS3Path);
                  if (!config.getDisableAcl()) {
                    copyRequest.acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);
                  }
                  s3Client.copyObject(copyRequest.build());
                  s3Client.deleteObject(DeleteObjectRequest.builder().bucket(s3Bucket).key(s3Path).build());
                }
              } else {
                // ensure object exists in target location
                if (isObjectInBucket(targetS3Bucket, targetS3Path)) {
                  log.info(
                      "Not moving file [s3://%s/%s], already present in target location [s3://%s/%s]",
                      s3Bucket, s3Path,
                      targetS3Bucket, targetS3Path
                  );
                } else {
                  throw new SegmentLoadingException(
                      "Unable to move file [s3://%s/%s] to [s3://%s/%s], not present in either source or target location",
                      s3Bucket,
                      s3Path,
                      targetS3Bucket,
                      targetS3Path
                  );
                }
              }
              return null;
            }
          }
      );
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, SdkException.class);
      Throwables.propagateIfInstanceOf(e, SegmentLoadingException.class);
      throw Throwables.propagate(e);
    }
  }

  private boolean isObjectInBucket(final String bucket, final String key)
  {
    try {
      s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
      return true;
    }
    catch (NoSuchKeyException e) {
      return false;
    }
  }
}
