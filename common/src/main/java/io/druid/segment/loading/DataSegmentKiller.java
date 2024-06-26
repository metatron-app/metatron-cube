/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;

import java.io.IOException;

/**
 */
public interface DataSegmentKiller
{
  void kill(DataSegment segment) throws SegmentLoadingException;
  void killAll() throws IOException;

  default void killQuietly(Iterable<DataSegment> segments, Logger LOG)
  {
    for (DataSegment segment : segments) {
      try {
        kill(segment);
      }
      catch (Throwable t) {
        LOG.info("Failed to delete %s", segment);
      }
    }
  }
}
