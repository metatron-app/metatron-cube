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

package io.druid.server.coordinator.helper;

import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.java.util.common.logger.Logger;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import java.util.Set;

/**
 */
public class DruidCoordinatorCleanupUnneeded implements DruidCoordinatorHelper
{
  private static final Logger log = new Logger(DruidCoordinatorCleanupUnneeded.class);

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if (!params.isMajorTick()) {
      return params;
    }
    CoordinatorStats stats = new CoordinatorStats();
    Set<DataSegment> availableSegments = params.getMaterializedSegments();

    // Drop segments that no longer exist in the available segments configuration, *if* it has been populated. (It might
    // not have been loaded yet since it's filled asynchronously. But it's also filled atomically, so if there are any
    // segments at all, we should have all of them.)
    // Note that if metadata store has no segments, then availableSegments will stay empty and nothing will be dropped.
    // This is done to prevent a race condition in which the coordinator would drop all segments if it started running
    // cleanup before it finished polling the metadata storage for available segments for the first time.
    if (!availableSegments.isEmpty()) {
      DruidCluster cluster = params.getDruidCluster();
      for (MinMaxPriorityQueue<ServerHolder> serverHolders : cluster.getSortedServersByTier()) {
        for (ServerHolder serverHolder : serverHolders) {
          ImmutableDruidServer server = serverHolder.getServer();
          LoadQueuePeon queuePeon = params.getLoadManagementPeons().get(server.getName());

          for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
            for (DataSegment segment : dataSource.getSegments()) {
              if (!availableSegments.contains(segment)) {
                if (!queuePeon.getSegmentsToDrop().contains(segment)) {
                  queuePeon.dropSegment(segment, "cleanup", null);
                  stats.addToTieredStat("unneededCount", server.getTier(), 1);
                }
              }
            }
          }
        }
      }
    } else {
      log.info(
          "Found 0 availableSegments, skipping the cleanup of segments from historicals. This is done to prevent a race condition in which the coordinator would drop all segments if it started running cleanup before it finished polling the metadata storage for available segments for the first time."
      );
    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }


}
