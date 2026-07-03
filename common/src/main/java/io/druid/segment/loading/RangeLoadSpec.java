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

package io.druid.segment.loading;

import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;

import java.io.IOException;

/**
 * A {@link LoadSpec} that can also serve a segment WITHOUT downloading the whole thing. It exposes the small
 * header bundle (version.bin + meta.smoosh + index.drd + metadata.drd — one small GET) for a header-first boot,
 * plus a {@link SmooshedFileMapper.RangeFetcher} that fetches individual column byte ranges on demand.
 *
 * A historical configured with {@code druid.segmentCache.loadMode=range} uses these (via
 * {@code SmooshedFileMapper.fromHeader}) instead of {@link #loadSegment}, so cold columns are read from deep
 * storage only when a query touches them (no local disk). LoadSpec types that can't range (e.g. {@code s3_zip})
 * simply don't implement this, and fall back to the normal download path.
 */
public interface RangeLoadSpec extends LoadSpec
{
  /** The header bundle bytes for this segment (one small GET). */
  byte[] header() throws IOException;

  /** A fetcher that GETs exactly the requested byte range of a chunk (fileNum, offset, length). */
  SmooshedFileMapper.RangeFetcher rangeFetcher();
}
