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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.client.DruidDataSource;
import io.druid.common.DateTimes;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.metadata.MetadataRuleManager;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class DruidCoordinatorRuntimeParams
{
  private final long startTime;
  private final long pollingInterval;
  private final DruidCluster druidCluster;
  private final MetadataRuleManager databaseRuleManager;
  private final SegmentReplicantLookup segmentReplicantLookup;
  private final Set<DruidDataSource> dataSources;
  private final Supplier<Map<String, List<DataSegment>>> availableSegments;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ServiceEmitter emitter;
  private final CoordinatorDynamicConfig coordinatorDynamicConfig;
  private final CoordinatorStats stats;
  private final BalancerStrategy balancerStrategy;
  private final boolean majorTick;

  private volatile boolean stopNow;

  private Map<String, Set<DataSegment>> materializedOvershadowedSegments;
  private Map<String, Iterable<DataSegment>> materializedNonOvershadowedSegments;

  public DruidCoordinatorRuntimeParams(
      long startTime,
      long pollingInterval,
      DruidCluster druidCluster,
      MetadataRuleManager databaseRuleManager,
      SegmentReplicantLookup segmentReplicantLookup,
      Set<DruidDataSource> dataSources,
      Supplier<Map<String, List<DataSegment>>> availableSegments,
      Map<String, LoadQueuePeon> loadManagementPeons,
      ServiceEmitter emitter,
      CoordinatorDynamicConfig coordinatorDynamicConfig,
      CoordinatorStats stats,
      BalancerStrategy balancerStrategy,
      boolean majorTick
  )
  {
    this.startTime = startTime;
    this.pollingInterval = pollingInterval;
    this.druidCluster = druidCluster;
    this.databaseRuleManager = databaseRuleManager;
    this.segmentReplicantLookup = segmentReplicantLookup;
    this.dataSources = dataSources;
    this.availableSegments = availableSegments;
    this.loadManagementPeons = loadManagementPeons;
    this.emitter = emitter;
    this.coordinatorDynamicConfig = coordinatorDynamicConfig;
    this.stats = stats;
    this.balancerStrategy = balancerStrategy;
    this.majorTick = majorTick;
  }

  public boolean isMajorTick()
  {
    return majorTick;
  }

  public void stopNow()
  {
    this.stopNow = true;
  }

  public boolean isStopNow()
  {
    return stopNow;
  }

  public long getStartTime()
  {
    return startTime;
  }

  public DruidCluster getDruidCluster()
  {
    return druidCluster;
  }

  public MetadataRuleManager getDatabaseRuleManager()
  {
    return databaseRuleManager;
  }

  public SegmentReplicantLookup getSegmentReplicantLookup()
  {
    return segmentReplicantLookup;
  }

  public Set<DruidDataSource> getDataSources()
  {
    return dataSources;
  }

  public Map<String, List<DataSegment>> getAvailableSegments()
  {
    return availableSegments.get();
  }

  public Map<String, Set<DataSegment>> getOvershadowedSegments()
  {
    if (materializedOvershadowedSegments == null) {
      materializedOvershadowedSegments = ImmutableMap.copyOf(retainOverShadowed(getAvailableSegments()));
    }
    return materializedOvershadowedSegments;
  }

  public Map<String, Iterable<DataSegment>> getNonOvershadowedSegments()
  {
    if (materializedNonOvershadowedSegments == null) {
      materializedNonOvershadowedSegments = ImmutableMap.copyOf(
          retainNonOverShadowed(getAvailableSegments(), getOvershadowedSegments())
      );
    }
    return materializedNonOvershadowedSegments;
  }

  private Map<String, Set<DataSegment>> retainOverShadowed(Map<String, List<DataSegment>> availables)
  {
    Map<String, Set<DataSegment>> overshadows = Maps.newHashMap();
    for (Map.Entry<String, List<DataSegment>> entry : availables.entrySet()) {
      VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>();
      for (DataSegment segment : entry.getValue()) {
        timeline.add(
            segment.getInterval(), segment.getVersion(), segment.getShardSpecWithDefault().createChunk(segment)
        );
      }
      Set<DataSegment> overshadow = Sets.newHashSet();
      for (TimelineObjectHolder<String, DataSegment> holder : timeline.findOvershadowed()) {
        for (DataSegment dataSegment : holder.getObject().payloads()) {
          overshadow.add(dataSegment);
        }
      }
      if (!overshadow.isEmpty()) {
        overshadows.put(entry.getKey(), overshadow);
      }
    }
    return overshadows;
  }

  private static Map<String, List<DataSegment>> retainNonOverShadowed(
      Map<String, List<DataSegment>> availables,
      Map<String, Set<DataSegment>> overshadows
  )
  {
    Map<String, List<DataSegment>> nonOvershadows = Maps.newHashMap();
    for (Map.Entry<String, List<DataSegment>> entry : availables.entrySet()) {
      String ds = entry.getKey();
      List<DataSegment> segments = entry.getValue();
      Set<DataSegment> overshadow = overshadows.get(ds);
      if (overshadow == null || overshadow.isEmpty()) {
        nonOvershadows.put(ds, segments);
        continue;
      }
      List<DataSegment> nonOvershadow = Lists.newArrayList();
      for (DataSegment segment : segments) {
        if (!overshadow.contains(segment)) {
          nonOvershadow.add(segment);
        }
      }
      if (!nonOvershadow.isEmpty()) {
        nonOvershadows.put(ds, nonOvershadow);
      }
    }
    return nonOvershadows;
  }

  public Map<String, LoadQueuePeon> getLoadManagementPeons()
  {
    return loadManagementPeons;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public CoordinatorDynamicConfig getCoordinatorDynamicConfig()
  {
    return coordinatorDynamicConfig;
  }

  public CoordinatorStats getCoordinatorStats()
  {
    return stats;
  }

  public BalancerStrategy getBalancerStrategy()
  {
    return balancerStrategy;
  }

  public boolean hasDeletionWaitTimeElapsed()
  {
    return DateTimes.elapsed(startTime) > coordinatorDynamicConfig.getMillisToWaitBeforeDeleting();
  }

  public boolean hasPollinIntervalElapsed(long startTime)
  {
    return pollingInterval > 0 && DateTimes.elapsed(startTime) > (majorTick ? pollingInterval << 1 : pollingInterval);
  }

  public static Builder newBuilder()
  {
    return new Builder();
  }

  public Builder buildFromExisting()
  {
    Builder builder = new Builder(
        majorTick,
        startTime,
        druidCluster,
        databaseRuleManager,
        segmentReplicantLookup,
        dataSources,
        availableSegments,
        loadManagementPeons,
        emitter,
        coordinatorDynamicConfig,
        stats,
        balancerStrategy
    );
    builder.materializedOvershadowedSegments = materializedOvershadowedSegments;
    builder.materializedNonOvershadowedSegments = materializedNonOvershadowedSegments;
    return builder;
  }

  public int getMaxSegmentsToMove()
  {
    final int maxSegmentsToMove = coordinatorDynamicConfig.getMaxSegmentsToMove();
    return maxSegmentsToMove < 0 ? Math.max(4, loadManagementPeons.size()) : maxSegmentsToMove;
  }

  public static class Builder
  {
    private long startTime;
    private long pollingInterval;
    private DruidCluster druidCluster;
    private MetadataRuleManager databaseRuleManager;
    private SegmentReplicantLookup segmentReplicantLookup;
    private final Set<DruidDataSource> dataSources;
    private Supplier<Map<String, List<DataSegment>>> availableSegments;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private ServiceEmitter emitter;
    private CoordinatorDynamicConfig coordinatorDynamicConfig;
    private final CoordinatorStats stats;
    private BalancerStrategy balancerStrategy;
    private boolean majorTick = true;     // test compatible

    private Map<String, Set<DataSegment>> materializedOvershadowedSegments;
    private Map<String, Iterable<DataSegment>> materializedNonOvershadowedSegments;

    Builder()
    {
      this.startTime = 0;
      this.druidCluster = null;
      this.databaseRuleManager = null;
      this.segmentReplicantLookup = null;
      this.dataSources = Sets.newHashSet();
      this.availableSegments = null;
      this.loadManagementPeons = Maps.newHashMap();
      this.emitter = null;
      this.stats = new CoordinatorStats();
      this.coordinatorDynamicConfig = CoordinatorDynamicConfig.DEFAULT;
    }

    Builder(
        boolean majorTick,
        long startTime,
        DruidCluster cluster,
        MetadataRuleManager databaseRuleManager,
        SegmentReplicantLookup segmentReplicantLookup,
        Set<DruidDataSource> dataSources,
        Supplier<Map<String, List<DataSegment>>> availableSegments,
        Map<String, LoadQueuePeon> loadManagementPeons,
        ServiceEmitter emitter,
        CoordinatorDynamicConfig coordinatorDynamicConfig,
        CoordinatorStats stats,
        BalancerStrategy balancerStrategy
    )
    {
      this.majorTick = majorTick;
      this.startTime = startTime;
      this.druidCluster = cluster;
      this.databaseRuleManager = databaseRuleManager;
      this.segmentReplicantLookup = segmentReplicantLookup;
      this.dataSources = dataSources;
      this.availableSegments = availableSegments;
      this.loadManagementPeons = loadManagementPeons;
      this.emitter = emitter;
      this.coordinatorDynamicConfig = coordinatorDynamicConfig;
      this.stats = stats;
      this.balancerStrategy = balancerStrategy;
    }

    public DruidCoordinatorRuntimeParams build()
    {
      DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams(
          startTime,
          pollingInterval,
          druidCluster,
          databaseRuleManager,
          segmentReplicantLookup,
          dataSources,
          availableSegments,
          loadManagementPeons,
          emitter,
          coordinatorDynamicConfig,
          stats,
          balancerStrategy,
          majorTick
      );
      params.materializedOvershadowedSegments = materializedOvershadowedSegments;
      params.materializedNonOvershadowedSegments = materializedNonOvershadowedSegments;
      return params;
    }

    public Builder withMajorTick(boolean tick)
    {
      majorTick = tick;
      return this;
    }

    public Builder withStartTime(long time)
    {
      startTime = time;
      return this;
    }

    public Builder withPollingInterval(long pollingInterval)
    {
      this.pollingInterval = pollingInterval;
      return this;
    }

    public Builder withDruidCluster(DruidCluster cluster)
    {
      this.druidCluster = cluster;
      return this;
    }

    public Builder withDatabaseRuleManager(MetadataRuleManager databaseRuleManager)
    {
      this.databaseRuleManager = databaseRuleManager;
      return this;
    }

    public Builder withSegmentReplicantLookup(SegmentReplicantLookup lookup)
    {
      this.segmentReplicantLookup = lookup;
      return this;
    }

    public Builder withDatasources(Collection<DruidDataSource> dataSourcesCollection)
    {
      dataSources.addAll(Collections.unmodifiableCollection(dataSourcesCollection));
      return this;
    }

    public Builder withAvailableSegments(Supplier<Map<String, List<DataSegment>>> availableSegments)
    {
      this.availableSegments = Suppliers.memoize(availableSegments);
      materializedOvershadowedSegments = null;
      materializedNonOvershadowedSegments = null;
      return this;
    }

    @VisibleForTesting
    public Builder withAvailableSegments(Collection<DataSegment> availableSegments)
    {
      final Map<String, List<DataSegment>> mapping = Maps.newHashMap();
      for (DataSegment segment : availableSegments) {
        mapping.compute(segment.getDataSource(), (k, v) -> {
          if (v == null) {
            v = Lists.newArrayList(segment);
          } else {
            v.add(segment);
          }
          return v;
        });
      }
      return withAvailableSegments(Suppliers.ofInstance(mapping));
    }

    public Builder withLoadManagementPeons(Map<String, LoadQueuePeon> loadManagementPeonsCollection)
    {
      loadManagementPeons.putAll(Collections.unmodifiableMap(loadManagementPeonsCollection));
      return this;
    }

    public Builder withEmitter(ServiceEmitter emitter)
    {
      this.emitter = emitter;
      return this;
    }

    public Builder withCoordinatorStats(CoordinatorStats stats)
    {
      this.stats.accumulate(stats);
      return this;
    }

    public Builder withDynamicConfigs(CoordinatorDynamicConfig configs)
    {
      this.coordinatorDynamicConfig = configs;
      return this;
    }

    public Builder withBalancerStrategy(BalancerStrategy balancerStrategy)
    {
      this.balancerStrategy = balancerStrategy;
      return this;
    }
  }
}
