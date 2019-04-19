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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.emitter.EmittingLogger;
import io.druid.metadata.MetadataRuleManager;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.SegmentReplicantLookup;
import io.druid.server.coordinator.ServerHolder;
import io.druid.server.coordinator.rules.Rule;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 */
public class DruidCoordinatorRuleRunner implements DruidCoordinatorHelper
{
  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorRuleRunner.class);
  private static final int MAX_MISSING_RULES = 10;
  private static final int MAX_NOT_ASSIGNED = 100;

  private final DruidCoordinator coordinator;

  public DruidCoordinatorRuleRunner(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    ReplicationThrottler replicatorThrottler = getReplicationThrottler(params);
    replicatorThrottler.updateParams(
        coordinator.getDynamicConfigs().getReplicationThrottleLimit(),
        coordinator.getDynamicConfigs().getReplicantLifetime()
    );

    CoordinatorStats stats = new CoordinatorStats();
    DruidCluster cluster = params.getDruidCluster();

    if (cluster.isEmpty()) {
      log.warn("Uh... I have no servers. Not assigning anything...");
      return params;
    }

    for (String tier : cluster.getTierNames()) {
      replicatorThrottler.updateReplicationState(tier);
      replicatorThrottler.updateTerminationState(tier);
    }

    DruidCoordinatorRuntimeParams paramsWithReplicationManager = params.buildFromExisting()
                                                                       .withCoordinatorStats(stats)
                                                                       .withReplicationManager(replicatorThrottler)
                                                                       .build();

    // Run through all matched rules for available segments
    final DateTime now = new DateTime();
    final MetadataRuleManager databaseRuleManager = paramsWithReplicationManager.getDatabaseRuleManager();

    final List<String> segmentsWithMissingRules = Lists.newArrayListWithCapacity(MAX_MISSING_RULES);
    final Map<String, List<Rule>> rulesPerDataSource = Maps.newHashMap();
    int missingRules = 0;
    int notAssignedCount = 0;
    final Set<DataSegment> targetSegments = getTargetSegments(paramsWithReplicationManager);
    final int maxNotAssigned = Math.max(10, Math.min((int)(targetSegments.size() * 0.3), MAX_NOT_ASSIGNED));
    for (DataSegment segment : targetSegments) {
      List<Rule> rules = rulesPerDataSource.computeIfAbsent(
          segment.getDataSource(), new Function<String, List<Rule>>()
          {
            @Override
            public List<Rule> apply(String dataSource)
            {
              return databaseRuleManager.getRulesWithDefault(dataSource);
            }
          }
      );
      boolean notAssigned = true;
      boolean foundMatchingRule = false;
      for (Rule rule : rules) {
        if (rule.appliesTo(segment, now)) {
          notAssigned &= rule.run(coordinator, paramsWithReplicationManager, segment);
          foundMatchingRule = true;
          break;
        }
      }

      if (!foundMatchingRule) {
        if (segmentsWithMissingRules.size() < MAX_MISSING_RULES) {
          segmentsWithMissingRules.add(segment.getIdentifier());
        }
        missingRules++;
      } else if (notAssigned && notAssignedCount++ >= maxNotAssigned) {
        logCluster(cluster);
        break;
      }
    }

    if (!segmentsWithMissingRules.isEmpty()) {
      log.makeAlert("Unable to find matching rules!")
         .addData("segmentsWithMissingRulesCount", missingRules)
         .addData("segmentsWithMissingRules", segmentsWithMissingRules)
         .emit();
    }

    return paramsWithReplicationManager;
  }

  private void logCluster(DruidCluster cluster)
  {
    log.warn("Some segments are not assigned.. something wrong?");
    for (Map.Entry<String, MinMaxPriorityQueue<ServerHolder>> entry : cluster.getCluster().entrySet()) {
      String tier = entry.getKey();
      MinMaxPriorityQueue<ServerHolder> servers = entry.getValue();
      List<String> full = Lists.newArrayList(Iterables.transform(Iterables.filter(
          servers,
          new Predicate<ServerHolder>()
          {
            @Override
            public boolean apply(ServerHolder holder)
            {
              return holder.getAvailableSize() < (1L << 28);
            }
          }
      ), new com.google.common.base.Function<ServerHolder, String>()
      {
        @Override
        public String apply(ServerHolder input)
        {
          return input.getServer().getName();
        }
      }));
      if (!full.isEmpty()) {
        log.warn(
            "tier[%s] : total %d servers, %d servers full %s", tier, servers.size(), full.size(),
            full.size() > 10 ? full.subList(0, 10) : full
        );
      }
    }
  }

  protected ReplicationThrottler getReplicationThrottler(DruidCoordinatorRuntimeParams params)
  {
    return params.getReplicationManager();
  }

  protected Set<DataSegment> getTargetSegments(DruidCoordinatorRuntimeParams coordinatorParam)
  {
    if (!coordinatorParam.isMajorTick()) {
      final SegmentReplicantLookup replicantLookup = coordinatorParam.getSegmentReplicantLookup();
      return coordinator.makeOrdered(
          Iterables.filter(
              coordinatorParam.getNonOvershadowedSegments(), new Predicate<DataSegment>()
              {
                @Override
                public boolean apply(DataSegment input)
                {
                  return replicantLookup.getTotalReplicants(input.getIdentifier()) == 0;
                }
              }
          )
      );
    }
    return coordinatorParam.getNonOvershadowedSegments();
  }
}
