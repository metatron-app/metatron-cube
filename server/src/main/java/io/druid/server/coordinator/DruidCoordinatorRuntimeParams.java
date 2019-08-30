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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.DruidDataSource;
import io.druid.metadata.MetadataRuleManager;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.DateTime;

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
  private final DruidCluster druidCluster;
  private final MetadataRuleManager databaseRuleManager;
  private final SegmentReplicantLookup segmentReplicantLookup;
  private final Set<DruidDataSource> dataSources;
  private final Iterable<DataSegment> availableSegments;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ServiceEmitter emitter;
  private final CoordinatorDynamicConfig coordinatorDynamicConfig;
  private final CoordinatorStats stats;
  private final DateTime balancerReferenceTimestamp;
  private final BalancerStrategy balancerStrategy;
  private final boolean majorTick;

  private Set<DataSegment> materializedSegments;
  private Set<DataSegment> materializedOvershadowedSegments;
  private Set<DataSegment> materializedNonOvershadowedSegments;

  public DruidCoordinatorRuntimeParams(
      long startTime,
      DruidCluster druidCluster,
      MetadataRuleManager databaseRuleManager,
      SegmentReplicantLookup segmentReplicantLookup,
      Set<DruidDataSource> dataSources,
      Iterable<DataSegment> availableSegments,
      Map<String, LoadQueuePeon> loadManagementPeons,
      ServiceEmitter emitter,
      CoordinatorDynamicConfig coordinatorDynamicConfig,
      CoordinatorStats stats,
      DateTime balancerReferenceTimestamp,
      BalancerStrategy balancerStrategy,
      boolean majorTick
  )
  {
    this.startTime = startTime;
    this.druidCluster = druidCluster;
    this.databaseRuleManager = databaseRuleManager;
    this.segmentReplicantLookup = segmentReplicantLookup;
    this.dataSources = dataSources;
    this.availableSegments = availableSegments;
    this.loadManagementPeons = loadManagementPeons;
    this.emitter = emitter;
    this.coordinatorDynamicConfig = coordinatorDynamicConfig;
    this.stats = stats;
    this.balancerReferenceTimestamp = balancerReferenceTimestamp;
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
    return materializedSegments != null ? materializedSegments : availableSegments;
  }

  public Set<DataSegment> getMaterializedSegments()
  {
    if (materializedSegments == null) {
      materializedSegments = ImmutableSet.copyOf(availableSegments);
    }
    return materializedSegments;
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

  public DateTime getBalancerReferenceTimestamp()
  {
    return balancerReferenceTimestamp;
  }

  public BalancerStrategy getBalancerStrategy()
  {
    return balancerStrategy;
  }

  public boolean hasDeletionWaitTimeElapsed()
  {
    return (System.currentTimeMillis() - getStartTime() > coordinatorDynamicConfig.getMillisToWaitBeforeDeleting());
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
        balancerReferenceTimestamp,
        balancerStrategy
    );
    builder.materializedSegments = materializedSegments;
    builder.materializedOvershadowedSegments = materializedOvershadowedSegments;
    builder.materializedNonOvershadowedSegments = materializedNonOvershadowedSegments;
    return builder;
  }

  public int getMaxSegmentsToMove()
  {
    final int maxSegmentsToMove = coordinatorDynamicConfig.getMaxSegmentsToMove();
    return maxSegmentsToMove < 0 ? Math.min(32, Math.max(4, loadManagementPeons.size() >> 1)) : maxSegmentsToMove;
  }

  public static class Builder
  {
    private long startTime;
    private DruidCluster druidCluster;
    private MetadataRuleManager databaseRuleManager;
    private SegmentReplicantLookup segmentReplicantLookup;
    private final Set<DruidDataSource> dataSources;
    private Iterable<DataSegment> availableSegments;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private ServiceEmitter emitter;
    private CoordinatorDynamicConfig coordinatorDynamicConfig;
    private CoordinatorStats stats;
    private DateTime balancerReferenceTimestamp;
    private BalancerStrategy balancerStrategy;
    private boolean majorTick = true;     // test compatible

    private Set<DataSegment> materializedSegments;
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
      this.coordinatorDynamicConfig = new CoordinatorDynamicConfig.Builder().build();
      this.balancerReferenceTimestamp = DateTime.now();
    }

    Builder(
        boolean majorTick,
        long startTime,
        DruidCluster cluster,
        MetadataRuleManager databaseRuleManager,
        SegmentReplicantLookup segmentReplicantLookup,
        Set<DruidDataSource> dataSources,
        Iterable<DataSegment> availableSegments,
        Map<String, LoadQueuePeon> loadManagementPeons,
        ServiceEmitter emitter,
        CoordinatorDynamicConfig coordinatorDynamicConfig,
        CoordinatorStats stats,
        DateTime balancerReferenceTimestamp,
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
      this.balancerReferenceTimestamp = balancerReferenceTimestamp;
      this.balancerStrategy = balancerStrategy;
    }

    public DruidCoordinatorRuntimeParams build()
    {
      DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams(
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
          balancerReferenceTimestamp,
          balancerStrategy,
          majorTick
      );
      params.materializedSegments = materializedSegments;
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

    public Builder withAvailableSegments(Iterable<DataSegment> availableSegments)
    {
      this.availableSegments = availableSegments;
      materializedSegments = materializedOvershadowedSegments = materializedNonOvershadowedSegments = null;
      return this;
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

    public Builder withBalancerReferenceTimestamp(DateTime balancerReferenceTimestamp)
    {
      this.balancerReferenceTimestamp = balancerReferenceTimestamp;
      return this;
    }

    public Builder withBalancerStrategy(BalancerStrategy balancerStrategy)
    {
      this.balancerStrategy = balancerStrategy;
      return this;
    }
  }
}
