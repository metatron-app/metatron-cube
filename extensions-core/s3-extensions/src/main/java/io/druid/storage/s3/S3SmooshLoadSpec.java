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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.segment.loading.LoadSpec;
import io.druid.segment.loading.SegmentLoadingException;

import java.io.File;

/** LoadSpec for the unzipped {@code s3_smoosh} container (separate header + chunk objects under a prefix). */
@JsonTypeName("s3_smoosh")
public class S3SmooshLoadSpec implements LoadSpec
{
  @JsonProperty(S3SmooshDataSegmentPuller.BUCKET)
  private final String bucket;
  @JsonProperty(S3SmooshDataSegmentPuller.PREFIX)
  private final String prefix;

  private final S3SmooshDataSegmentPuller puller;

  @JsonCreator
  public S3SmooshLoadSpec(
      @JacksonInject S3SmooshDataSegmentPuller puller,
      @JsonProperty(S3SmooshDataSegmentPuller.BUCKET) String bucket,
      @JsonProperty(S3SmooshDataSegmentPuller.PREFIX) String prefix
  )
  {
    this.bucket = Preconditions.checkNotNull(bucket);
    this.prefix = Preconditions.checkNotNull(prefix);
    this.puller = puller;
  }

  @Override
  public LoadSpecResult loadSegment(File outDir) throws SegmentLoadingException
  {
    return new LoadSpecResult(puller.getSegmentFiles(bucket, prefix, outDir).size());
  }
}
