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

import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.List;

/**
 * Discovers segments straight from deep storage (no coordinator, no metadata DB) for the standalone historical:
 * lists a datasource's segment prefix and parses each {@code descriptor.json} into a {@link DataSegment}. The
 * deep-storage-specific implementation (e.g. S3) is bound by the storage extension's module; consumed by the
 * standalone bootstrap. In {@code common} so a storage extension can implement it without depending on server.
 */
public interface SegmentScanner
{
  /**
   * All segment descriptors found in deep storage for the configured datasource(s). Overshadowed (superseded)
   * versions are filtered by the caller via a VersionedIntervalTimeline before loading.
   */
  List<DataSegment> scan() throws IOException;
}
