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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.java.util.common.Throwables;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;

/**
 * Pusher for the unzipped {@code s3_smoosh} container: instead of one {@code index.zip}, upload the segment's
 * files as separate objects under a prefix — a readable {@code header} bundle (version.bin + meta.smoosh +
 * index.drd + metadata.drd) plus the raw {@code NNNNN.smoosh} chunks. No compression/zip, so reads can range
 * only what they need. Sets loadSpec {@code {"type":"s3_smoosh","bucket":..,"prefix":..}}. Coexists with s3_zip.
 */
public class S3SmooshDataSegmentPusher implements DataSegmentPusher
{
  private static final EmittingLogger log = new EmittingLogger(S3SmooshDataSegmentPusher.class);

  private final S3Client s3Client;
  private final S3DataSegmentPusherConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public S3SmooshDataSegmentPusher(S3Client s3Client, S3DataSegmentPusherConfig config, ObjectMapper jsonMapper)
  {
    this.s3Client = s3Client;
    this.config = config;
    this.jsonMapper = jsonMapper;
    log.info("Configured S3 (smoosh/unzipped) as deep storage");
  }

  @Override
  public String getPathForHadoop()
  {
    return String.format("s3n://%s/%s", config.getBucket(), config.getBaseKey());
  }

  @Deprecated
  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment inSegment) throws IOException
  {
    final String zipPath = S3Utils.constructSegmentPath(config.getBaseKey(), inSegment);   // <dir>/index.zip
    final String prefix = zipPath.substring(0, zipPath.lastIndexOf('/'));
    final String bucket = config.getBucket();
    log.info("Copying segment[%s] to S3 (smoosh) at prefix[%s]", inSegment.getIdentifier(), prefix);
    try {
      return S3Utils.retryS3Operation(() -> {
        long total = 0;

        final byte[] header = SmooshedFileMapper.writeHeader(indexFilesDir);
        putBytes(bucket, prefix + "/header", header);
        total += header.length;

        for (File f : indexFilesDir.listFiles()) {
          if (f.isFile() && f.getName().matches("\\d+\\.smoosh")) {   // the raw chunk objects
            putFile(bucket, prefix + "/" + f.getName(), f);
            total += f.length();
          }
        }

        final DataSegment outSegment = inSegment
            .withSize(total)
            .withLoadSpec(ImmutableMap.<String, Object>of("type", "s3_smoosh", "bucket", bucket, "prefix", prefix))
            .withBinaryVersion(SegmentUtils.getVersionFromDir(indexFilesDir));

        putBytes(bucket, prefix + "/descriptor.json", jsonMapper.writeValueAsBytes(outSegment));
        return outSegment;
      });
    }
    catch (SdkException e) {
      throw new IOException(e);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void putBytes(String bucket, String key, byte[] bytes)
  {
    s3Client.putObject(builder(bucket, key).build(), RequestBody.fromBytes(bytes));
  }

  private void putFile(String bucket, String key, File file)
  {
    s3Client.putObject(builder(bucket, key).build(), RequestBody.fromFile(file.toPath()));
  }

  private PutObjectRequest.Builder builder(String bucket, String key)
  {
    final PutObjectRequest.Builder b = PutObjectRequest.builder().bucket(bucket).key(key);
    if (!config.getDisableAcl()) {
      b.acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);
    }
    return b;
  }
}
