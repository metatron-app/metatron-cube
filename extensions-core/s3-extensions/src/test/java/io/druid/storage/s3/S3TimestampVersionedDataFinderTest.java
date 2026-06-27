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

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.time.Instant;
import java.util.regex.Pattern;

public class S3TimestampVersionedDataFinderTest
{

  @Test
  public void testSimpleLatestVersion()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    S3Client s3Client = EasyMock.createStrictMock(S3Client.class);

    S3Object object0 = S3Object.builder()
                               .key(keyPrefix + "/renames-0.gz")
                               .lastModified(Instant.ofEpochMilli(0))
                               .build();
    S3Object object1 = S3Object.builder()
                               .key(keyPrefix + "/renames-1.gz")
                               .lastModified(Instant.ofEpochMilli(1))
                               .build();

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.<ListObjectsV2Request>anyObject())).andReturn(
        ListObjectsV2Response.builder().contents(object0, object1).build()
    ).once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(String.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    URI expected = URI.create(String.format("s3://%s/%s", bucket, object1.key()));

    Assert.assertEquals(expected, latest);
  }

  @Test
  public void testMissing()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    S3Client s3Client = EasyMock.createStrictMock(S3Client.class);

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.<ListObjectsV2Request>anyObject())).andReturn(
        ListObjectsV2Response.builder().build()
    ).once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(String.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    Assert.assertEquals(null, latest);
  }

  @Test
  public void testFindSelf()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    S3Client s3Client = EasyMock.createStrictMock(S3Client.class);

    S3Object object0 = S3Object.builder()
                               .key(keyPrefix + "/renames-0.gz")
                               .lastModified(Instant.ofEpochMilli(0))
                               .build();

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.<ListObjectsV2Request>anyObject())).andReturn(
        ListObjectsV2Response.builder().contents(object0).build()
    ).once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(String.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    URI expected = URI.create(String.format("s3://%s/%s", bucket, object0.key()));

    Assert.assertEquals(expected, latest);
  }

  @Test
  public void testFindExact()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    S3Client s3Client = EasyMock.createStrictMock(S3Client.class);

    S3Object object0 = S3Object.builder()
                               .key(keyPrefix + "/renames-0.gz")
                               .lastModified(Instant.ofEpochMilli(0))
                               .build();

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.<ListObjectsV2Request>anyObject())).andReturn(
        ListObjectsV2Response.builder().contents(object0).build()
    ).once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);


    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(String.format("s3://%s/%s", bucket, object0.key())), null);

    EasyMock.verify(s3Client);

    URI expected = URI.create(String.format("s3://%s/%s", bucket, object0.key()));

    Assert.assertEquals(expected, latest);
  }
}
