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

package io.druid.server.coordinator.rules;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.SegmentReplicantLookup;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 * LoadRules indicate the number of replicants a segment should have in a given tier.
 */
public abstract class LoadRule implements Rule
{
  private static final EmittingLogger log = new EmittingLogger(LoadRule.class);
  private static final String assignedCount = "assignedCount";
  private static final String droppedCount = "droppedCount";

  @Override
  public boolean run(DruidCoordinator coordinator, DruidCoordinatorRuntimeParams params, DataSegment segment)
  {
    final Pair<Long, List<String>> fails = coordinator.getRecentlyFailedServers(segment);
    if (fails != null && fails.rhs.size() > 3) {
      log.info("Skip segment [%s] for recent [%s] fails on %s", segment, DateTimes.utc(fails.lhs), fails.rhs);
      return false;
    }
    final String segmentId = segment.getIdentifier();
    final CoordinatorStats stats = params.getCoordinatorStats();

    final DruidCluster cluster = params.getDruidCluster();
    final SegmentReplicantLookup replicantLookup = params.getSegmentReplicantLookup();

    boolean assignedAny = false;
    int totalReplicantsInCluster = replicantLookup.getTotalReplicants(segmentId);
    final Map<String, Integer> tieredReplicants = getTieredReplicants();
    for (Map.Entry<String, Integer> entry : tieredReplicants.entrySet()) {
      final String tier = entry.getKey();
      final MinMaxPriorityQueue<ServerHolder> serverQueue = cluster.getServersByTier(tier);
      if (GuavaUtils.isNullOrEmpty(serverQueue)) {
        continue;
      }
      final int expectedReplicantsInTier = entry.getValue();
      final int totalReplicantsInTier = replicantLookup.getTotalReplicants(segmentId, tier);
      if (totalReplicantsInTier >= expectedReplicantsInTier) {
        continue;
      }

      List<ServerHolder> serverHolderList = Lists.newArrayList(serverQueue);
      if (fails != null) {
        final Map<String, ServerHolder> serverHolderMap = Maps.newHashMap();
        for (ServerHolder holder : serverHolderList) {
          serverHolderMap.put(holder.getServer().getName(), holder);
        }
        for (String server : fails.rhs) {
          serverHolderMap.remove(server);
        }
        if (serverHolderMap.isEmpty()) {
          continue;
        }
        serverHolderList = Lists.newArrayList(serverHolderMap.values());
        if (serverHolderList.isEmpty()) {
          continue;
        }
      }

      int assigned = assign(
          tier,
          segment,
          totalReplicantsInCluster,
          expectedReplicantsInTier,
          totalReplicantsInTier,
          params.getBalancerStrategy(),
          serverHolderList
      );
      if (assigned > 0) {
        stats.addToTieredStat(assignedCount, tier, assigned);
        totalReplicantsInCluster += assigned;
        assignedAny = true;
      }
    }
    if (!assignedAny) {
      // Remove over-replication
      drop(segment, params, tieredReplicants);
    }
    return totalReplicantsInCluster == 0;
  }

  private int assign(
      final String tier,
      final DataSegment segment,
      final int totalReplicantsInCluster,
      final int expectedReplicantsInTier,
      final int totalReplicantsInTier,
      final BalancerStrategy strategy,
      final List<ServerHolder> serverHolderList
  )
  {
    int assigned = 0;
    int currReplicantsInTier = totalReplicantsInTier;
    int currTotalReplicantsInCluster = totalReplicantsInCluster;
    while (currReplicantsInTier < expectedReplicantsInTier) {

      final ServerHolder holder = strategy.findNewSegmentHomeReplicator(segment, serverHolderList);
      if (holder == null) {
        log.warn(
            "Not enough servers or node capacity in tier [%s] to assign segment[%s]! Expected Replicants[%d]",
            tier,
            segment.getIdentifier(),
            expectedReplicantsInTier
        );
        break;
      }

      if (assign(
          segment,
          holder.getPeon(),
          StringUtils.safeFormat("under-replicated(%d/%d)", currReplicantsInTier, expectedReplicantsInTier)
      )) {
        ++assigned;
        ++currReplicantsInTier;
        ++currTotalReplicantsInCluster;
      }
    }

    return assigned;
  }

  protected boolean assign(DataSegment segment, LoadQueuePeon peon, String reason)
  {
    return peon.loadSegment(segment, reason, null, null);
  }

  private void drop(DataSegment segment, DruidCoordinatorRuntimeParams params, Map<String, Integer> tieredReplicants)
  {
    // Find all instances of this segment across tiers
    final SegmentReplicantLookup lookup = params.getSegmentReplicantLookup();
    final Map<String, int[]> replicantsByTier = lookup.getClusterTiers(segment.getIdentifier());

    final CoordinatorStats stats = params.getCoordinatorStats();
    for (Map.Entry<String, int[]> entry : replicantsByTier.entrySet()) {
      String tier = entry.getKey();
      MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().get(tier);
      if (serverQueue == null || serverQueue.isEmpty()) {
        log.makeAlert("No holders found for tier[%s]", tier).emit();
        continue;
      }
      final int expectedNumReplicantsForTier = tieredReplicants.getOrDefault(tier, 0);
      final int dropped = drop(expectedNumReplicantsForTier, segment, entry.getValue(), serverQueue);
      if (dropped > 0) {
        stats.addToTieredStat(droppedCount, tier, dropped);
      }
    }
  }

  private int drop(
      final int expectedNumReplicantsForTier,
      final DataSegment segment,
      final int[] numReplicantsForTier,
      final MinMaxPriorityQueue<ServerHolder> serverQueue
  )
  {
    int currentNumReplicantsForTier = numReplicantsForTier[SegmentReplicantLookup.LOADED];
    if (currentNumReplicantsForTier <= expectedNumReplicantsForTier) {
      return 0;
    }

    int dropped = 0;
    List<ServerHolder> droppedServers = Lists.newArrayList();
    while (currentNumReplicantsForTier > expectedNumReplicantsForTier) {
      final ServerHolder holder = serverQueue.pollLast();
      if (holder.isServingSegment(segment) && holder.getPeon().dropSegment(
          segment,
          StringUtils.safeFormat("over-replicated(%d/%d)", currentNumReplicantsForTier, expectedNumReplicantsForTier),
          null,
          null
      )) {
        --currentNumReplicantsForTier;
        ++dropped;
      }
      droppedServers.add(holder);
    }
    serverQueue.addAll(droppedServers);
    return dropped;
  }

  protected void validateTieredReplicants(Map<String, Integer> tieredReplicants)
  {
    if (tieredReplicants.isEmpty()) {
      throw new IAE("A rule with empty tiered replicants is invalid");
    }
    for (Map.Entry<String, Integer> entry : tieredReplicants.entrySet()) {
      if (entry.getValue() == null) {
        throw new IAE("Replicant value cannot be empty");
      }
      if (entry.getValue() < 0) {
        throw new IAE("Replicant value [%d] is less than 0, which is not allowed", entry.getValue());
      }
    }
  }

  public abstract Map<String, Integer> getTieredReplicants();

  protected abstract static class Always extends LoadRule
  {
    @Override
    public final boolean appliesTo(Interval interval, DateTime referenceTimestamp)
    {
      return true;
    }
  }
}
