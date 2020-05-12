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

package io.druid.metadata;

import com.google.common.collect.ImmutableList;
import io.druid.java.util.common.Pair;
import io.druid.client.DruidDataSource;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;

/**
 */

public interface MetadataSegmentManager
{
  void start();

  void stop();

  // register to temporary view.. can be removed by polling if it's not inserted to db till then
  boolean registerToView(DataSegment segment);

  boolean unregisterFromView(DataSegment segment);

  boolean enableDatasource(String ds, boolean now);

  boolean enableSegment(String segmentId, boolean now);

  boolean disableDatasource(String ds);

  boolean disableSegment(String ds, String segmentID);

  int disableSegments(String ds, Interval interval);

  boolean isStarted();

  TableDesc getDataSourceDesc(String ds);

  DruidDataSource getInventoryValue(String key);

  ImmutableList<DruidDataSource> getInventory();

  ImmutableList<String> getAllDatasourceNames();

  Pair<String, DataSegment> getLastUpdatedSegment(String ds);

  DateTime lastUpdatedTime();

  /**
   * Returns top N unused segment intervals in given interval when ordered by segment start time, end time.
   */
  List<Interval> getUnusedSegmentIntervals(
      final String dataSource,
      final Interval interval,
      final int limit
  );

  Interval getUmbrellaInterval(String ds);

  void poll();
}
