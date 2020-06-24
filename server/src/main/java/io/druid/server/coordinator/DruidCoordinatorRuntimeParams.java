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
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
  private final Supplier<Iterable<DataSegment>> availableSegments;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ServiceEmitter emitter;
  private final CoordinatorDynamicConfig coordinatorDynamicConfig;
  private final CoordinatorStats stats;
  private final BalancerStrategy balancerStrategy;
  private final boolean majorTick;

  private Set<DataSegment> materializedOvershadowedSegments;
  private Set<DataSegment> materializedNonOvershadowedSegments;

  public DruidCoordinatorRuntimeParams(
      long startTime,
      long pollingInterval,
      DruidCluster druidCluster,
      MetadataRuleManager databaseRuleManager,
      SegmentReplicantLookup segmentReplicantLookup,
      Set<DruidDataSource> dataSources,
      Supplier<Iterable<DataSegment>> availableSegments,
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

  public Iterable<DataSegment> getAvailableSegments()
  {
    return availableSegments.get();
  }

  public Set<DataSegment> getOvershadowedSegments()
  {
    if (materializedOvershadowedSegments == null) {
      materializedOvershadowedSegments = Collections.unmodifiableSet(retainOverShadowed(getAvailableSegments()));
    }
    return materializedOvershadowedSegments;
  }

  public Set<DataSegment> getNonOvershadowedSegments()
  {
    if (materializedNonOvershadowedSegments == null) {
      materializedNonOvershadowedSegments = Collections.unmodifiableSet(
          retainNonOverShadowed(getAvailableSegments(), getOvershadowedSegments())
      );
    }
    return materializedNonOvershadowedSegments;
  }

  private Set<DataSegment> retainOverShadowed(Iterable<DataSegment> segments)
  {
    Map<String, VersionedIntervalTimeline<String, DataSegment>> timelines = new HashMap<>();
    for (DataSegment segment : segments) {
      VersionedIntervalTimeline<String, DataSegment> timeline = timelines.computeIfAbsent(
          segment.getDataSource(), new Function<String, VersionedIntervalTimeline<String, DataSegment>>()
          {
            @Override
            public VersionedIntervalTimeline<String, DataSegment> apply(String dataSource)
            {
              return new VersionedIntervalTimeline<>(Ordering.natural());
            }
          });
      timeline.add(
          segment.getInterval(), segment.getVersion(), segment.getShardSpecWithDefault().createChunk(segment)
      );
    }

    Set<DataSegment> overshadowed = new HashSet<>();
    for (VersionedIntervalTimeline<String, DataSegment> timeline : timelines.values()) {
      for (TimelineObjectHolder<String, DataSegment> holder : timeline.findOvershadowed()) {
        for (DataSegment dataSegment : holder.getObject().payloads()) {
          overshadowed.add(dataSegment);
        }
      }
    }
    return overshadowed;
  }

  private Set<DataSegment> retainNonOverShadowed(Iterable<DataSegment> segments, Set<DataSegment> overshadowed)
  {
    Set<DataSegment> nonOvershadowed = new HashSet<>();
    for (DataSegment dataSegment : segments) {
      if (!overshadowed.contains(dataSegment)) {
        nonOvershadowed.add(dataSegment);
      }
    }
    return nonOvershadowed;
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
    private Supplier<Iterable<DataSegment>> availableSegments;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private ServiceEmitter emitter;
    private CoordinatorDynamicConfig coordinatorDynamicConfig;
    private CoordinatorStats stats;
    private BalancerStrategy balancerStrategy;
    private boolean majorTick = true;     // test compatible

    private Set<DataSegment> materializedOvershadowedSegments;
    private Set<DataSegment> materializedNonOvershadowedSegments;

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
        Supplier<Iterable<DataSegment>> availableSegments,
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

    public Builder withAvailableSegments(Supplier<Iterable<DataSegment>> availableSegments)
    {
      this.availableSegments = Suppliers.memoize(availableSegments);
      materializedOvershadowedSegments = materializedNonOvershadowedSegments = null;
      return this;
    }

    @VisibleForTesting
    public Builder withAvailableSegments(Iterable<DataSegment> availableSegments)
    {
      return withAvailableSegments(Suppliers.ofInstance(availableSegments));
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
