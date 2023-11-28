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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.common.DateTimes;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.metadata.MetadataRuleManager;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.SegmentReplicantLookup;
import io.druid.server.coordinator.ServerHolder;
import io.druid.server.coordinator.rules.Rule;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class DruidCoordinatorRuleRunner implements DruidCoordinatorHelper
{
  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorRuleRunner.class);
  private static final int MAX_MISSING_RULES = 10;
  private static final int MAX_NOT_ASSIGNED = 100;

  private static final int TIMEOUT_CHECK_INTERVAL = 2000;

  private final DruidCoordinator coordinator;

  public DruidCoordinatorRuleRunner(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final DruidCluster cluster = params.getDruidCluster();
    if (cluster.isEmpty()) {
      log.warn("Uh... I have no servers. Not assigning anything...");
      return params;
    }

    // Run through all matched rules for available segments
    final DateTime now = DateTimes.nowUtc();
    final MetadataRuleManager databaseRuleManager = params.getDatabaseRuleManager();

    final List<String> segmentsWithMissingRules = Lists.newArrayListWithCapacity(MAX_MISSING_RULES);
    int segments = 0;
    int missingRules = 0;
    int notAssignedCount = 0;
    for (Map.Entry<String, Iterable<DataSegment>> entry : getTargetSegments(params).entrySet()) {
      List<Rule> rules = databaseRuleManager.getRulesWithDefault(entry.getKey());
      boolean assigned = false;
      boolean foundMatchingRule = false;
      for (DataSegment segment : entry.getValue()) {
        segments++;
        for (Rule rule : rules) {
          if (rule.appliesTo(segment, now)) {
            assigned |= rule.run(coordinator, params, segment);
            foundMatchingRule = true;
            break;
          }
        }

        if (!foundMatchingRule) {
          if (segmentsWithMissingRules.size() < MAX_MISSING_RULES) {
            segmentsWithMissingRules.add(segment.getIdentifier());
          }
          missingRules++;
        } else if (!assigned) {
          notAssignedCount++;
        }
        if (segments % TIMEOUT_CHECK_INTERVAL == 0 && (params.isStopNow() || params.hasPollinIntervalElapsed(now.getMillis()))) {
          break;
        }
      }
    }
    final int maxNotAssigned = Math.max(10, Math.min((int)(segments * 0.3), MAX_NOT_ASSIGNED));
    if (notAssignedCount >= maxNotAssigned) {
      logCluster(cluster, notAssignedCount);
    }
    if (!segmentsWithMissingRules.isEmpty()) {
      log.makeAlert("Unable to find matching rules!")
         .addData("segmentsWithMissingRulesCount", missingRules)
         .addData("segmentsWithMissingRules", segmentsWithMissingRules)
         .emit();
    }

    return params;
  }

  private void logCluster(DruidCluster cluster, int notAssignedCount)
  {
    log.warn("Some [%d] segments are not assigned.. something wrong?", notAssignedCount);
    for (Map.Entry<String, MinMaxPriorityQueue<ServerHolder>> entry : cluster.getCluster().entrySet()) {
      String tier = entry.getKey();
      MinMaxPriorityQueue<ServerHolder> servers = entry.getValue();
      List<String> full = Lists.newArrayList(
          Iterables.transform(
              Iterables.filter(servers, holder -> holder.getAvailableSize() < (1L << 28)),
              ServerHolder::getName)
      );
      if (!full.isEmpty()) {
        log.warn(
            "tier[%s] : total %d servers, %d servers full %s", tier, servers.size(), full.size(),
            full.size() > 10 ? full.subList(0, 10) : full
        );
      }
    }
  }

  protected Map<String, Iterable<DataSegment>> getTargetSegments(DruidCoordinatorRuntimeParams coordinatorParam)
  {
    Set<DataSegment> blacklist = coordinator.getBlacklisted(true);
    Map<String, Iterable<DataSegment>> segments = coordinatorParam.getNonOvershadowedSegments();
    if (!blacklist.isEmpty()) {
      log.debug("Blacklist.. %s", Logger.lazy(() -> forLog(blacklist)));
      segments = Maps.transformValues(
          segments, list -> Iterables.filter(list, segment -> !blacklist.contains(segment))
      );
    }
    SegmentReplicantLookup replicantLookup = coordinatorParam.getSegmentReplicantLookup();
    if (!coordinatorParam.isMajorTick()) {
      segments = Maps.transformValues(
          segments,
          list -> Iterables.filter(list, segment -> replicantLookup.getTotalReplicants(segment.getIdentifier()) == 0)
      );
    }
    return segments;
  }

  private static final int THRESHOLD = 10;

  private static String forLog(Set<DataSegment> blacklist)
  {
    if (blacklist.isEmpty()) {
      return "[]";
    }
    int limit = Math.min(blacklist.size(), THRESHOLD);
    Iterator<DataSegment> iterator = blacklist.iterator();
    StringBuilder b = new StringBuilder();
    b.append('[').append(iterator.next().getIdentifier());
    for (int i = 1; i < limit; i++) {
      b.append(", ").append(iterator.next().getIdentifier());
    }
    if (blacklist.size() > THRESHOLD) {
      b.append(",.. ").append(blacklist.size() - THRESHOLD).append(" more]");
    } else {
      b.append(']');
    }
    return b.toString();
  }
}
