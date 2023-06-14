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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.SegmentReplicantLookup;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
    final Set<String> failed = coordinator.getFailedServers(segment);
    final String segmentId = segment.getIdentifier();
    final CoordinatorStats stats = params.getCoordinatorStats();

    final DruidCluster cluster = params.getDruidCluster();
    final SegmentReplicantLookup replicantLookup = params.getSegmentReplicantLookup();

    final int maxLoad = params.getMaxPendingSegmentsToLoad();

    boolean assignedAny = false;
    int totalReplicantsInCluster = replicantLookup.getTotalReplicants(segmentId);
    final Map<String, Integer> tieredReplicants = getTieredReplicants();
    for (Map.Entry<String, Integer> entry : tieredReplicants.entrySet()) {
      final String tier = entry.getKey();
      final MinMaxPriorityQueue<ServerHolder> serverQueue = cluster.get(tier);
      if (GuavaUtils.isNullOrEmpty(serverQueue)) {
        continue;
      }
      final int expectedReplicantsInTier = entry.getValue();
      final int totalReplicantsInTier = replicantLookup.getTotalReplicants(segmentId, tier);
      if (totalReplicantsInTier >= expectedReplicantsInTier) {
        continue;
      }
      final List<ServerHolder> servers = filterServers(serverQueue, failed, maxLoad);
      if (!failed.isEmpty() && servers.isEmpty()) {
        coordinator.releaseFailedServers(segment);
        continue;
      }

      int assigned = assign(
          coordinator,
          tier,
          segment,
          totalReplicantsInTier,
          expectedReplicantsInTier,
          params.getBalancerStrategy(),
          servers,
          !failed.isEmpty()
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

  private List<ServerHolder> filterServers(
      Iterable<ServerHolder> servers,
      Set<String> fails,
      int maxPendingSegmentsToLoad
  )
  {
    if (!fails.isEmpty()) {
      servers = Iterables.filter(servers, server -> !fails.contains(server.getName()));
    }
    if (maxPendingSegmentsToLoad > 0) {
      servers = Iterables.filter(servers, server -> server.getNumSegmentsToLoad() < maxPendingSegmentsToLoad);
    }
    return Lists.newArrayList(servers);
  }

  private int assign(
      final DruidCoordinator coordinator,
      final String tier,
      final DataSegment segment,
      final int totalReplicantsInTier,
      final int expectedReplicantsInTier,
      final BalancerStrategy strategy,
      final List<ServerHolder> serverHolderList,
      final boolean failedBefore
  )
  {
    int assigned = 0;
    int currReplicantsInTier = totalReplicantsInTier;
    while (currReplicantsInTier < expectedReplicantsInTier) {

      final ServerHolder holder = strategy.findNewSegmentHomeReplicator(segment, serverHolderList);
      if (holder == null) {
        if (Iterables.all(serverHolderList, h -> h.isDecommissioned() || h.getAvailableSize() < segment.getSize())) {
          log.warn(
              "Not enough servers or node capacity in tier [%s] to assign segment[%s]! Expected Replicants[%d]",
              tier,
              segment.getIdentifier(),
              expectedReplicantsInTier
          );
        }
        break;
      }

      String reason = String.format("under-replicated(%d/%d)", currReplicantsInTier, expectedReplicantsInTier);
      if (assign(segment, holder.getPeon(), reason, null)) {
        ++assigned;
        ++currReplicantsInTier;
      }
    }

    return assigned;
  }

  protected boolean assign(DataSegment segment, LoadQueuePeon peon, String reason, LoadPeonCallback callback)
  {
    peon.loadSegment(segment, reason, callback);
    return true;
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
      if (GuavaUtils.isNullOrEmpty(serverQueue)) {
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
    while (!serverQueue.isEmpty() && currentNumReplicantsForTier > expectedNumReplicantsForTier) {
      final ServerHolder holder = Preconditions.checkNotNull(serverQueue.pollLast());
      if (holder.isServingSegment(segment)) {
        String reason = String.format("over-replicated(%d/%d)", currentNumReplicantsForTier, expectedNumReplicantsForTier);
        holder.getPeon().dropSegment(segment, reason, null);
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
