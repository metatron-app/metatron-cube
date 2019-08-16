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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Set;

/**
 */
public class DataSegmentPusherUtil
{
  private static final Joiner JOINER = Joiner.on("/").skipNulls();
  private static final Set<String> HDFS_DIR_SCHEMES = ImmutableSet.of("hdfs", "viewfs", "wasb", "wasbs");

  public static String getStorageDir(String scheme, DataSegment segment)
  {
    return HDFS_DIR_SCHEMES.contains(scheme) ? getHdfsStorageDir(segment) : getStorageDir(segment);
  }

  public static String getStorageDir(DataSegment segment)
  {
    return getStorageDir(segment, null);
  }

  // Note: storage directory structure format = .../dataSource/interval/version/partitionNumber/
  // If above format is ever changed, make sure to change it appropriately in other places
  // e.g. HDFSDataSegmentKiller uses this information to clean the version, interval and dataSource directories
  // on segment deletion if segment being deleted was the only segment
  public static String getStorageDir(DataSegment segment, DateTimeZone timezone)
  {
    Interval interval = segment.getInterval();
    if (timezone != null) {
      interval = interval.withChronology(interval.getChronology().withZone(timezone));
    }
    return JOINER.join(
        segment.getDataSource(),
        String.format(
            "%s_%s",
            interval.getStart(),
            interval.getEnd()
        ),
        segment.getVersion(),
        segment.getShardSpecWithDefault().getPartitionNum()
    );
  }

  /**
   * Due to https://issues.apache.org/jira/browse/HDFS-13 ":" are not allowed in
   * path names. So we format paths differently for HDFS.
   */
  public static String getHdfsStorageDir(DataSegment segment)
  {
    return JOINER.join(
        segment.getDataSource(),
        String.format(
            "%s_%s",
            segment.getInterval().getStart().toString(ISODateTimeFormat.basicDateTime()),
            segment.getInterval().getEnd().toString(ISODateTimeFormat.basicDateTime())
        ),
        segment.getVersion().replaceAll(":", "_"),
        segment.getShardSpecWithDefault().getPartitionNum()
    );
  }
}
