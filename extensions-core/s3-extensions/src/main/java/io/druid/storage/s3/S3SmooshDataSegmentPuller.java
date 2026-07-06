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

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.ContainerHeader;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Puller for the unzipped {@code s3_smoosh} container: a segment is stored as separate objects under a prefix —
 * a readable {@code header} bundle (version.bin + meta.smoosh + index.drd + metadata.drd) plus the raw
 * {@code NNNNN.smoosh} chunk objects. Two modes:
 *  - {@link #getSegmentFiles} (download): reconstruct a v9 segment dir (version.bin + meta.smoosh from the header,
 *    then the chunk objects) so the existing mmap load path works unchanged.
 *  - {@link #rangeFetcher} (range): a header-first / column-range serving path (no whole download) for lazy load.
 */
public class S3SmooshDataSegmentPuller implements DataSegmentPuller
{
  private static final Logger log = new Logger(S3SmooshDataSegmentPuller.class);

  static final String BUCKET = "bucket";
  static final String PREFIX = "prefix";

  private final S3Client s3Client;

  @Inject
  public S3SmooshDataSegmentPuller(S3Client s3Client)
  {
    this.s3Client = s3Client;
  }

  @Override
  public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException
  {
    final Map<String, Object> loadSpec = segment.getLoadSpec();
    getSegmentFiles(MapUtils.getString(loadSpec, BUCKET), MapUtils.getString(loadSpec, PREFIX), outDir);
  }

  public FileUtils.FileCopyResult getSegmentFiles(String bucket, String prefix, File outDir) throws SegmentLoadingException
  {
    if (!outDir.exists()) {
      outDir.mkdirs();
    }
    if (!outDir.isDirectory()) {
      throw new ISE("outDir[%s] must be a directory.", outDir);
    }
    try {
      // header -> reconstruct the two top-level files a v9 dir needs (index.drd/metadata.drd stay inside the chunk)
      final byte[] headerBytes = getObject(bucket, prefix + "/header");
      final File version = new File(outDir, "version.bin");
      final File meta = new File(outDir, "meta.smoosh");
      final byte[] versionBin;
      final byte[] metaSmoosh;
      if (ContainerHeader.isV2(headerBytes)) {                          // readable v2 header
        versionBin = ContainerHeader.versionBin(headerBytes);
        metaSmoosh = ContainerHeader.metaSmoosh(headerBytes);
      } else {                                                          // legacy v1 packed header
        final Map<String, byte[]> parts = SmooshedFileMapper.unpackHeader(headerBytes);
        versionBin = parts.get("version.bin");
        metaSmoosh = parts.get("meta.smoosh");
      }
      Files.write(versionBin, version);
      Files.write(metaSmoosh, meta);
      final int chunks = SmooshedFileMapper.chunkCount(metaSmoosh);
      final File[] written = new File[chunks + 2];
      written[0] = version;
      written[1] = meta;
      for (int i = 0; i < chunks; i++) {
        final String name = SmooshedFileMapper.chunkName(i);
        final File chunk = new File(outDir, name);
        Files.write(getObject(bucket, prefix + "/" + name), chunk);
        written[i + 2] = chunk;
      }
      return new FileUtils.FileCopyResult(written);
    }
    catch (Exception e) {
      throw new SegmentLoadingException(e, "Unable to pull s3_smoosh segment from [%s/%s]", bucket, prefix);
    }
  }

  /** The header bundle bytes for a segment (one small GET) — for header-first / range serving. */
  public byte[] header(String bucket, String prefix) throws IOException
  {
    final byte[] header = getObject(bucket, prefix + "/header");
    log.debug("[s3_smoosh] header GET prefix[%s] len[%d]", prefix, header.length);
    return header;
  }

  /**
   * A {@link SmooshedFileMapper.RangeFetcher} that GETs exactly the requested byte range of a chunk object into a
   * DIRECT (off-heap) ByteBuffer. Range-served columns are long-lived and can be large; keeping them off the JVM
   * heap avoids GC pressure at scale (a heap buffer here would pile up as old-gen byte[]). The direct buffer is
   * freed by its Cleaner when the owning column becomes unreachable (segment drop/evict). Requires the historical's
   * -XX:MaxDirectMemorySize to cover the resident column set + processing buffers.
   */
  public SmooshedFileMapper.RangeFetcher rangeFetcher(String bucket, String prefix)
  {
    return (fileNum, offset, length) -> {
      final String key = prefix + "/" + SmooshedFileMapper.chunkName(fileNum);
      final String range = "bytes=" + offset + "-" + (offset + length - 1);
      log.debug("[s3_smoosh] range GET prefix[%s] chunk[%d] offset[%d] len[%d] (direct)", prefix, fileNum, offset, length);
      try (ResponseInputStream<GetObjectResponse> in =
               s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).range(range).build())) {
        final byte[] bytes = ByteStreams.toByteArray(in);   // transient heap staging (young-gen, freed fast)
        final ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
        direct.put(bytes).flip();
        return direct;
      }
    };
  }

  private byte[] getObject(String bucket, String key) throws IOException
  {
    try (ResponseInputStream<GetObjectResponse> in =
             s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())) {
      return ByteStreams.toByteArray(in);
    }
  }
}
