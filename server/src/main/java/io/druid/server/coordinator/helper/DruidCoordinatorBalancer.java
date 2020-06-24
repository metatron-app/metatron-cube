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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class DruidCoordinatorBalancer implements DruidCoordinatorHelper
{
  public static final Comparator<ServerHolder> percentUsedComparator = Comparators.inverse(
      new Comparator<ServerHolder>()
      {
        @Override
        public int compare(ServerHolder lhs, ServerHolder rhs)
        {
          return Double.compare(lhs.getPercentUsed(), rhs.getPercentUsed());
        }
      }
  );
  protected static final EmittingLogger log = new EmittingLogger(DruidCoordinatorBalancer.class);

  protected final DruidCoordinator coordinator;

  public DruidCoordinatorBalancer(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    // bulk moving of segments for decommissioned historical node
    handleDecommissionedServer(params);
    if (!params.isMajorTick()) {
      return params;
    }
    final BalancerStrategy strategy = params.getBalancerStrategy();
    final CoordinatorStats stats = new CoordinatorStats();

    for (Map.Entry<String, MinMaxPriorityQueue<ServerHolder>> entry :
        params.getDruidCluster().getCluster().entrySet()) {
      String tier = entry.getKey();

      final List<ServerHolder> holders = Lists.newArrayList(entry.getValue());
      if (holders.size() <= 1) {
        log.debug("[%s]: One or fewer servers found.  Cannot balance.", tier);
        continue;
      }

      int segmentsToLoad = 0;
      for (ServerHolder holder : holders) {
        segmentsToLoad += holder.getPeon().getNumSegmentsToLoad();
      }
      if (segmentsToLoad > params.getMaxSegmentsToMove()) {
        // skip when busy (server down, etc.)
        log.info("[%s]: Still waiting on %,d segments to be moved", tier, segmentsToLoad);
        continue;
      }

      int numSegments = 0;
      for (ServerHolder server : holders) {
        numSegments += server.getServer().getSegments().size();
      }

      if (numSegments == 0) {
        log.debug("No segments found.  Cannot balance.");
        continue;
      }

      int balanced = strategy.balance(holders, this, params);

      stats.addToTieredStat("movedCount", tier, balanced);
      if (params.getCoordinatorDynamicConfig().emitBalancingStats()) {
        strategy.emitStats(tier, stats, holders);
      }
      if (balanced > 0) {
        log.info("[%s] : Moved %d segments for balancing", tier, balanced);
      }
    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }

  private void handleDecommissionedServer(DruidCoordinatorRuntimeParams params)
  {
    final BalancerStrategy strategy = params.getBalancerStrategy();
    for (MinMaxPriorityQueue<ServerHolder> servers : params.getDruidCluster().getSortedServersByTier()) {
      List<ServerHolder> holders = Lists.newArrayList(servers);
      Iterator<ServerHolder> iterator = holders.iterator();
      Map<ServerHolder, List<DataSegment>> segmentsMap = Maps.newHashMap();
      while (iterator.hasNext()) {
        ServerHolder holder = iterator.next();
        if (holder.getServer().isAssignable()) {
          Map<String, DataSegment> segments = holder.getServer().getSegments();
          if (holder.isDecommissioned() && !segments.isEmpty()) {
            segmentsMap.put(holder, Lists.newArrayList(segments.values()));
            iterator.remove();
          }
        }
      }
      if (!segmentsMap.isEmpty() && !holders.isEmpty()) {
        for (Map.Entry<ServerHolder, List<DataSegment>> entry : segmentsMap.entrySet()) {
          for (DataSegment segment : entry.getValue()) {
            ServerHolder target = strategy.findNewSegmentHomeReplicator(segment, holders);
            if (target != null) {
              moveSegment(segment, entry.getKey(), target);
            }
          }
        }
      }
    }
  }

  public boolean moveSegment(final DataSegment segment, final ServerHolder fromServer, final ServerHolder toServer)
  {
    if (!toServer.isLoadingSegment(segment) &&
        !toServer.isServingSegment(segment) &&
        toServer.getAvailableSize() > segment.getSize()) {
      log.debug("Moving [%s] from [%s] to [%s]", segment.getIdentifier(), fromServer.getName(), toServer.getName());
      return coordinator.moveSegment(segment, fromServer, toServer, null, null);
    }
    return false;
  }

  public boolean isAvailable(DataSegment segment)
  {
    return coordinator.isAvailable(segment);
  }
}
