/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.guava.Comparators;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.ImmutableDruidServer;
import io.druid.data.Pair;
import io.druid.server.coordinator.BalancerSegmentHolder;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

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
          return lhs.getPercentUsed().compareTo(rhs.getPercentUsed());
        }
      }
  );
  protected static final EmittingLogger log = new EmittingLogger(DruidCoordinatorBalancer.class);

  protected final DruidCoordinator coordinator;

  protected final Map<String, ConcurrentHashMap<String, BalancerSegmentHolder>> currentlyMovingSegments = Maps.newHashMap();

  public DruidCoordinatorBalancer(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  protected void reduceLifetimes(String tier)
  {
    for (BalancerSegmentHolder holder : currentlyMovingSegments.get(tier).values()) {
      holder.reduceLifetime();
      if (holder.getLifetime() <= 0) {
        log.makeAlert("[%s]: Balancer move segments queue has a segment stuck", tier)
           .addData("segment", holder.getSegment().getIdentifier())
           .addData("server", holder.getFromServer().getMetadata())
           .emit();
      }
    }
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if (!params.isMajorTick()) {
      return params;
    }
    final CoordinatorStats stats = new CoordinatorStats();
    final BalancerStrategy strategy = params.getBalancerStrategy();

    for (Map.Entry<String, MinMaxPriorityQueue<ServerHolder>> entry :
        params.getDruidCluster().getCluster().entrySet()) {
      String tier = entry.getKey();

      final ConcurrentHashMap<String, BalancerSegmentHolder> tierMap = currentlyMovingSegments.computeIfAbsent(
          tier, new Function<String, ConcurrentHashMap<String, BalancerSegmentHolder>>()
          {
            @Override
            public ConcurrentHashMap<String, BalancerSegmentHolder> apply(String s)
            {
              return new ConcurrentHashMap<String, BalancerSegmentHolder>();
            }
          }
      );
      if (!tierMap.isEmpty()) {
        reduceLifetimes(tier);
        log.info("[%s]: Still waiting on %,d segments to be moved", tier, currentlyMovingSegments.size());
        continue;
      }

      final List<ServerHolder> serverHolderList = Lists.newArrayList(entry.getValue());
      int segmentsToLoad = 0;
      for (ServerHolder holder : serverHolderList) {
        segmentsToLoad += holder.getPeon().getSegmentsToLoad().size();
      }
      if (segmentsToLoad > params.getMaxSegmentsToMove() << 1) {
        // skip when busy (server down, etc.)
        continue;
      }

      if (serverHolderList.size() <= 1) {
        log.info("[%s]: One or fewer servers found.  Cannot balance.", tier);
        continue;
      }

      int numSegments = 0;
      for (ServerHolder server : serverHolderList) {
        numSegments += server.getServer().getSegments().size();
      }

      if (numSegments == 0) {
        log.info("No segments found.  Cannot balance.");
        continue;
      }

      for (Pair<BalancerSegmentHolder, ImmutableDruidServer> pair : strategy.select(params, serverHolderList)) {
        moveSegment(pair.lhs, pair.rhs, params);
      }
      stats.addToTieredStat("movedCount", tier, tierMap.size());
      if (params.getCoordinatorDynamicConfig().emitBalancingStats()) {
        strategy.emitStats(tier, stats, serverHolderList);
      }
      if (tierMap.size() > 0) {
        log.info("[%s]: Segments Moved: [%d]", tier, tierMap.size());
      }
    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }

  protected void moveSegment(
      final BalancerSegmentHolder segment,
      final ImmutableDruidServer toServer,
      final DruidCoordinatorRuntimeParams params
  )
  {
    final LoadQueuePeon toPeon = params.getLoadManagementPeons().get(toServer.getName());

    final ImmutableDruidServer fromServer = segment.getFromServer();
    final DataSegment segmentToMove = segment.getSegment();
    final String segmentName = segmentToMove.getIdentifier();

    if (!toPeon.getSegmentsToLoad().contains(segmentToMove) &&
        toServer.getSegment(segmentName) == null &&
        ServerHolder.getAvailableSize(toServer, toPeon) > segmentToMove.getSize()) {
      log.debug("Moving [%s] from [%s] to [%s]", segmentName, fromServer.getName(), toServer.getName());

      final Map<String, BalancerSegmentHolder> movingSegments = currentlyMovingSegments.get(toServer.getTier());
      final LoadPeonCallback callback = new LoadPeonCallback()
      {
        @Override
        public void execute()
        {
          movingSegments.remove(segmentName);
        }
      };
      movingSegments.put(segmentName, segment);

      try {
        coordinator.moveSegment(
            fromServer,
            toServer,
            segmentToMove.getIdentifier(),
            callback
        );
      }
      catch (Exception e) {
        log.makeAlert(e, String.format("[%s] : Moving exception", segmentName)).emit();
        callback.execute();
      }
    }
  }
}
