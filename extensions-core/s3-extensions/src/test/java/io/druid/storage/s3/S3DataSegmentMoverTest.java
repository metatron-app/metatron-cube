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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.java.util.common.MapUtils;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Map;
import java.util.Set;

public class S3DataSegmentMoverTest
{
  private static final DataSegment sourceSegment = new DataSegment(
      "test",
      new Interval("2013-01-01/2013-01-02"),
      "1",
      ImmutableMap.<String, Object>of(
          "key",
          "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
          "bucket",
          "main"
      ),
      ImmutableList.of("dim1", "dim1"),
      ImmutableList.of("metric1", "metric2"),
      NoneShardSpec.instance(),
      0,
      1
  );

  @Test
  public void testMove() throws Exception
  {
    MockS3Client mockS3Client = new MockS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());

    mockS3Client.put("main", "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip");
    mockS3Client.put("main", "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/descriptor.json");

    DataSegment movedSegment = mover.move(
        sourceSegment,
        ImmutableMap.<String, Object>of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = movedSegment.getLoadSpec();
    Assert.assertEquals("targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip", MapUtils.getString(targetLoadSpec, "key"));
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertTrue(mockS3Client.didMove());
  }

  @Test
  public void testMoveNoop() throws Exception
  {
    MockS3Client mockS3Client = new MockS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());

    mockS3Client.put("archive", "targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip");
    mockS3Client.put("archive", "targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/descriptor.json");

    DataSegment movedSegment = mover.move(
        sourceSegment,
        ImmutableMap.<String, Object>of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = movedSegment.getLoadSpec();

    Assert.assertEquals("targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip", MapUtils.getString(targetLoadSpec, "key"));
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertFalse(mockS3Client.didMove());
  }

  @Test(expected = SegmentLoadingException.class)
  public void testMoveException() throws Exception
  {
    MockS3Client mockS3Client = new MockS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());

    mover.move(
        sourceSegment,
        ImmutableMap.<String, Object>of("baseKey", "targetBaseKey", "bucket", "archive")
    );
  }

  @Test
  public void testIgnoresGoneButAlreadyMoved() throws Exception
  {
    MockS3Client mockS3Client = new MockS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());
    mover.move(new DataSegment(
        "test",
        new Interval("2013-01-01/2013-01-02"),
        "1",
        ImmutableMap.<String, Object>of(
            "key",
            "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
            "bucket",
            "DOES NOT EXIST"
        ),
        ImmutableList.of("dim1", "dim1"),
        ImmutableList.of("metric1", "metric2"),
        NoneShardSpec.instance(),
        0,
        1
    ), ImmutableMap.<String, Object>of("bucket", "DOES NOT EXIST", "baseKey", "baseKey"));
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFailsToMoveMissing() throws Exception
  {
    MockS3Client mockS3Client = new MockS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());
    mover.move(new DataSegment(
        "test",
        new Interval("2013-01-01/2013-01-02"),
        "1",
        ImmutableMap.<String, Object>of(
            "key",
            "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
            "bucket",
            "DOES NOT EXIST"
        ),
        ImmutableList.of("dim1", "dim1"),
        ImmutableList.of("metric1", "metric2"),
        NoneShardSpec.instance(),
        0,
        1
    ), ImmutableMap.<String, Object>of("bucket", "DOES NOT EXIST", "baseKey", "baseKey2"));
  }

  private static class MockS3Client implements S3Client
  {
    Map<String, Set<String>> storage = Maps.newHashMap();
    boolean copied = false;

    public boolean didMove()
    {
      return copied;
    }

    void put(String bucket, String key)
    {
      storage.computeIfAbsent(bucket, b -> Sets.<String>newHashSet()).add(key);
    }

    private boolean exists(String bucket, String key)
    {
      Set<String> objects = storage.get(bucket);
      return objects != null && objects.contains(key);
    }

    @Override
    public String serviceName()
    {
      return S3Client.SERVICE_NAME;
    }

    @Override
    public void close()
    {
    }

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest request)
    {
      if (exists(request.bucket(), request.key())) {
        return HeadObjectResponse.builder().build();
      }
      throw NoSuchKeyException.builder().message("Not found").build();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request)
    {
      final ListObjectsV2Response.Builder builder = ListObjectsV2Response.builder();
      if (exists(request.bucket(), request.prefix())) {
        builder.contents(
            S3Object.builder().key(request.prefix()).storageClass(ObjectStorageClass.STANDARD).build()
        );
      }
      return builder.build();
    }

    @Override
    public CopyObjectResponse copyObject(CopyObjectRequest request)
    {
      copied = true;
      if (exists(request.sourceBucket(), request.sourceKey())) {
        put(request.destinationBucket(), request.destinationKey());
      }
      return CopyObjectResponse.builder().build();
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest request)
    {
      Set<String> objects = storage.get(request.bucket());
      if (objects != null) {
        objects.remove(request.key());
      }
      return DeleteObjectResponse.builder().build();
    }
  }
}
