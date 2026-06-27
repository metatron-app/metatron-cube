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

import io.druid.java.util.common.FileUtils;
import io.druid.segment.loading.SegmentLoadingException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
public class S3DataSegmentPullerTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSimpleGetVersion() throws IOException
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    S3Client s3Client = EasyMock.createStrictMock(S3Client.class);

    final String key = keyPrefix + "/renames-0.gz";

    EasyMock.expect(s3Client.headObject(EasyMock.<HeadObjectRequest>anyObject()))
            .andReturn(HeadObjectResponse.builder().lastModified(Instant.ofEpochMilli(0)).build())
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);

    String version = puller.getVersion(URI.create(String.format("s3://%s/%s", bucket, key)));

    EasyMock.verify(s3Client);

    Assert.assertEquals(String.format("%d", 0L), version);
  }

  @Test
  public void testGZUncompress() throws IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final String key = keyPrefix + "/renames-0.gz";
    final S3Client s3Client = EasyMock.createStrictMock(S3Client.class);
    final byte[] value = bucket.getBytes("utf8");

    final File tmpFile = temporaryFolder.newFile("gzTest.gz");

    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    final File tmpDir = temporaryFolder.newFolder("gzTestDir");

    // isObjectInBucket() head + buildFileObject() head
    EasyMock.expect(s3Client.headObject(EasyMock.<HeadObjectRequest>anyObject()))
            .andReturn(HeadObjectResponse.builder().lastModified(Instant.ofEpochMilli(0)).build())
            .times(2);
    EasyMock.expect(s3Client.getObject(EasyMock.<GetObjectRequest>anyObject()))
            .andReturn(new ResponseInputStream<>(GetObjectResponse.builder().build(), new FileInputStream(tmpFile)))
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);
    FileUtils.FileCopyResult result = puller.getSegmentFiles(
        new S3DataSegmentPuller.S3Coords(
            bucket,
            key
        ), tmpDir
    );
    EasyMock.verify(s3Client);

    Assert.assertEquals(value.length, result.size());
    File expected = new File(tmpDir, "renames-0");
    Assert.assertTrue(expected.exists());
    Assert.assertEquals(value.length, expected.length());
  }

  @Test
  public void testGZUncompressRetries() throws IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final String key = keyPrefix + "/renames-0.gz";
    final S3Client s3Client = EasyMock.createStrictMock(S3Client.class);
    final byte[] value = bucket.getBytes("utf8");

    final File tmpFile = temporaryFolder.newFile("gzTest.gz");

    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    File tmpDir = temporaryFolder.newFolder("gzTestDir");

    final S3Exception exception = (S3Exception) S3Exception.builder()
                                                           .message("NoSuchKey")
                                                           .statusCode(404)
                                                           .build();

    // isObjectInBucket() head
    EasyMock.expect(s3Client.headObject(EasyMock.<HeadObjectRequest>anyObject()))
            .andReturn(HeadObjectResponse.builder().lastModified(Instant.ofEpochMilli(0)).build())
            .once();
    // first attempt: buildFileObject() head, then getObject throws (recoverable)
    EasyMock.expect(s3Client.headObject(EasyMock.<HeadObjectRequest>anyObject()))
            .andReturn(HeadObjectResponse.builder().lastModified(Instant.ofEpochMilli(0)).build())
            .once();
    EasyMock.expect(s3Client.getObject(EasyMock.<GetObjectRequest>anyObject()))
            .andThrow(exception)
            .once();
    // retry: buildFileObject() head, then getObject succeeds
    EasyMock.expect(s3Client.headObject(EasyMock.<HeadObjectRequest>anyObject()))
            .andReturn(HeadObjectResponse.builder().lastModified(Instant.ofEpochMilli(0)).build())
            .once();
    EasyMock.expect(s3Client.getObject(EasyMock.<GetObjectRequest>anyObject()))
            .andReturn(new ResponseInputStream<>(GetObjectResponse.builder().build(), new FileInputStream(tmpFile)))
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);
    FileUtils.FileCopyResult result = puller.getSegmentFiles(
        new S3DataSegmentPuller.S3Coords(
            bucket,
            key
        ), tmpDir
    );
    EasyMock.verify(s3Client);

    Assert.assertEquals(value.length, result.size());
    File expected = new File(tmpDir, "renames-0");
    Assert.assertTrue(expected.exists());
    Assert.assertEquals(value.length, expected.length());
  }

}
