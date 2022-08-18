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
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

/**
 */
public class DruidCoordinatorCleanupUnneeded implements DruidCoordinatorHelper
{
  private static final Logger log = new Logger(DruidCoordinatorCleanupUnneeded.class);

  private final DruidCoordinator coordinator;

  public DruidCoordinatorCleanupUnneeded(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    // effectively removes first race condition
    if (params.isStopNow() || !params.isMajorTick() || !coordinator.isReady()) {
      return params;
    }

    final DruidCluster cluster = params.getDruidCluster();
    final CoordinatorStats stats = params.getCoordinatorStats();

    for (MinMaxPriorityQueue<ServerHolder> serverHolders : cluster.getSortedServersByTier()) {
      for (ServerHolder serverHolder : serverHolders) {
        ImmutableDruidServer server = serverHolder.getServer();
        LoadQueuePeon queuePeon = params.getLoadManagementPeons().get(server.getName());

        for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
          for (DataSegment segment : dataSource.getSegments()) {
            if (!coordinator.isAvailable(segment) && !queuePeon.isDroppingSegment(segment)) {
              queuePeon.dropSegment(segment, "cleanup", null, null);
              stats.addToTieredStat("unneededCount", server.getTier(), 1);
            }
          }
        }
      }
    }

    return params;
  }
}
