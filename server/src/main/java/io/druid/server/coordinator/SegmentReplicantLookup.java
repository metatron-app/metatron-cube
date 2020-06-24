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

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.client.ImmutableDruidServer;
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A lookup for the number of replicants of a given segment for a certain tier.
 */
public interface SegmentReplicantLookup
{
  int LOADED = 0;
  int LOADING = 1;
  int[] NOT_EXISTS = new int[2];

  static SegmentReplicantLookup make(DruidCluster cluster)
  {
    final List<String> tierNames = Lists.newArrayList(cluster.getTierNames());
    if (tierNames.size() == 1) {
      final Map<String, int[]> segmentsInCluster = Maps.newHashMap();
      for (MinMaxPriorityQueue<ServerHolder> serversByType : cluster.getSortedServersByTier()) {
        for (ServerHolder serverHolder : serversByType) {
          final ImmutableDruidServer server = serverHolder.getServer();
          for (DataSegment segment : server.getSegments().values()) {
            segmentsInCluster.computeIfAbsent(segment.getIdentifier(), k -> new int[2])[LOADED]++;
          }

          // Also account for queued segments
          final LoadQueuePeon peon = serverHolder.getPeon();
          peon.getSegmentsToLoad(segment -> {
            segmentsInCluster.computeIfAbsent(segment.getIdentifier(), k -> new int[2])[LOADING]++;
          });
        }
      }
      return new SingleTier(tierNames.get(0), segmentsInCluster);
    }

    final Map<String, Map<String, int[]>> segmentsInCluster = Maps.newHashMap();
    for (MinMaxPriorityQueue<ServerHolder> serversByType : cluster.getSortedServersByTier()) {
      for (ServerHolder serverHolder : serversByType) {
        final ImmutableDruidServer server = serverHolder.getServer();
        for (DataSegment segment : server.getSegments().values()) {
          segmentsInCluster.computeIfAbsent(segment.getIdentifier(), k1 -> Maps.newHashMap())
                           .computeIfAbsent(server.getTier(), k -> new int[2])[LOADED]++;
        }

        // Also account for queued segments
        final LoadQueuePeon peon = serverHolder.getPeon();
        peon.getSegmentsToLoad(segment -> {
          segmentsInCluster.computeIfAbsent(segment.getIdentifier(), k1 -> Maps.newHashMap())
                           .computeIfAbsent(server.getTier(), k -> new int[2])[LOADING]++;
        });
      }
    }

    return new MultiTier(segmentsInCluster);
  }

  Map<String, int[]> getClusterTiers(String segmentId);

  int getTotalReplicants(String segmentId, String tier);

  int getTotalReplicants(String segmentId);

  class SingleTier implements SegmentReplicantLookup
  {
    private final String tierName;
    private final Map<String, int[]> segmentsInCluster;

    public SingleTier(String tierName, Map<String, int[]> segmentsInCluster)
    {
      this.tierName = tierName;
      this.segmentsInCluster = segmentsInCluster;
    }

    @Override
    public Map<String, int[]> getClusterTiers(String segmentId)
    {
      return ImmutableMap.of(tierName, segmentsInCluster.getOrDefault(segmentId, NOT_EXISTS));
    }

    @Override
    public int getTotalReplicants(String segmentId, String tier)
    {
      return tierName.equals(tier) ? getTotalReplicants(segmentId) : 0;
    }

    @Override
    public int getTotalReplicants(String segmentId)
    {
      final int[] counts = segmentsInCluster.getOrDefault(segmentId, NOT_EXISTS);
      return counts[LOADED] + counts[LOADING];
    }
  }

  class MultiTier implements SegmentReplicantLookup
  {
    private final Map<String, Map<String, int[]>> segmentsInCluster;

    private MultiTier(Map<String, Map<String, int[]>> segmentsInCluster)
    {
      this.segmentsInCluster = segmentsInCluster;
    }

    public Map<String, int[]> getClusterTiers(String segmentId)
    {
      return segmentsInCluster.getOrDefault(segmentId, Collections.emptyMap());
    }

    public int getTotalReplicants(String segmentId, String tier)
    {
      final int[] counts = getClusterTiers(segmentId).getOrDefault(tier, NOT_EXISTS);
      return counts[LOADED] + counts[LOADING];
    }

    public int getTotalReplicants(String segmentId)
    {
      int retVal = 0;
      for (int[] replicants : getClusterTiers(segmentId).values()) {
        retVal += replicants[LOADED] + replicants[LOADING];
      }
      return retVal;
    }
  }
}
