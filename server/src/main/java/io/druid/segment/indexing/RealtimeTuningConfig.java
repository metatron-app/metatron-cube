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

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.common.guava.Files;
import io.druid.segment.IndexSpec;
import io.druid.segment.incremental.BaseTuningConfig;
import io.druid.segment.realtime.appenderator.AppenderatorConfig;
import io.druid.segment.realtime.plumber.IntervalStartVersioningPolicy;
import io.druid.segment.realtime.plumber.RejectionPolicyFactory;
import io.druid.segment.realtime.plumber.ServerTimeRejectionPolicyFactory;
import io.druid.segment.realtime.plumber.VersioningPolicy;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Period;

import java.io.File;

/**
 */
@JsonTypeName("realtime")
public class RealtimeTuningConfig extends BaseTuningConfig implements AppenderatorConfig
{
  private static final Period defaultIntermediatePersistPeriod = new Period("PT10M");
  private static final Period defaultWindowPeriod = new Period("PT10M");
  private static final VersioningPolicy defaultVersioningPolicy = new IntervalStartVersioningPolicy();
  private static final RejectionPolicyFactory defaultRejectionPolicyFactory = new ServerTimeRejectionPolicyFactory();
  private static final int defaultMaxPendingPersists = 0;
  private static final ShardSpec defaultShardSpec = NoneShardSpec.instance();
  private static final Boolean defaultReportParseExceptions = Boolean.FALSE;
  private static final long defaultHandoffConditionTimeout = 0;

  private static final Boolean defaultIgnorePreviousSegments = Boolean.FALSE;

  private static File createNewBasePersistDirectory()
  {
    return Files.createTempDir();
  }

  // Might make sense for this to be a builder
  public static RealtimeTuningConfig makeDefaultTuningConfig(final File basePersistDirectory)
  {
    return new RealtimeTuningConfig(
        null,
        null,
        defaultIntermediatePersistPeriod,
        defaultWindowPeriod,
        basePersistDirectory == null ? createNewBasePersistDirectory() : basePersistDirectory,
        defaultVersioningPolicy,
        defaultRejectionPolicyFactory,
        defaultMaxPendingPersists,
        defaultShardSpec,
        null,
        null,
        0,
        0,
        defaultReportParseExceptions,
        defaultHandoffConditionTimeout,
        defaultIgnorePreviousSegments
    );
  }

  private final Period intermediatePersistPeriod;
  private final Period windowPeriod;
  private final File basePersistDirectory;
  private final VersioningPolicy versioningPolicy;
  private final RejectionPolicyFactory rejectionPolicyFactory;
  private final int maxPendingPersists;
  private final ShardSpec shardSpec;
  private final int persistThreadPriority;
  private final int mergeThreadPriority;
  private final boolean reportParseExceptions;
  private final long handoffConditionTimeout;

  private final boolean ignorePreviousSegments;

  @JsonCreator
  public RealtimeTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxOccupationInMemory") Long maxOccupationInMemory,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("versioningPolicy") VersioningPolicy versioningPolicy,
      @JsonProperty("rejectionPolicy") RejectionPolicyFactory rejectionPolicyFactory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("shardSpec") ShardSpec shardSpec,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("persistThreadPriority") int persistThreadPriority,
      @JsonProperty("mergeThreadPriority") int mergeThreadPriority,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout,
      @JsonProperty("ignorePreviousSegments") boolean ignorePreviousSegments
  )
  {
    super(indexSpec, maxRowsInMemory, maxOccupationInMemory, null, buildV9Directly, false);
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? defaultIntermediatePersistPeriod
                                     : intermediatePersistPeriod;
    this.windowPeriod = windowPeriod == null ? defaultWindowPeriod : windowPeriod;
    this.basePersistDirectory = basePersistDirectory == null ? createNewBasePersistDirectory() : basePersistDirectory;
    this.versioningPolicy = versioningPolicy == null ? defaultVersioningPolicy : versioningPolicy;
    this.rejectionPolicyFactory = rejectionPolicyFactory == null
                                  ? defaultRejectionPolicyFactory
                                  : rejectionPolicyFactory;
    this.maxPendingPersists = maxPendingPersists == null ? defaultMaxPendingPersists : maxPendingPersists;
    this.shardSpec = shardSpec == null ? defaultShardSpec : shardSpec;
    this.mergeThreadPriority = mergeThreadPriority;
    this.persistThreadPriority = persistThreadPriority;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? defaultReportParseExceptions
                                 : reportParseExceptions;
    this.handoffConditionTimeout = handoffConditionTimeout == null
                                   ? defaultHandoffConditionTimeout
                                   : handoffConditionTimeout;
    Preconditions.checkArgument(this.handoffConditionTimeout >= 0, "handoffConditionTimeout must be >= 0");
    this.ignorePreviousSegments = ignorePreviousSegments;
  }

  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }

  @JsonProperty
  public Period getWindowPeriod()
  {
    return windowPeriod;
  }

  @JsonProperty
  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

  @JsonProperty
  public VersioningPolicy getVersioningPolicy()
  {
    return versioningPolicy;
  }

  @JsonProperty("rejectionPolicy")
  public RejectionPolicyFactory getRejectionPolicyFactory()
  {
    return rejectionPolicyFactory;
  }

  @JsonProperty
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @JsonProperty
  public ShardSpec getShardSpec()
  {
    return shardSpec;
  }

  @JsonProperty
  public int getPersistThreadPriority()
  {
    return this.persistThreadPriority;
  }

  @JsonProperty
  public int getMergeThreadPriority()
  {
    return this.mergeThreadPriority;
  }

  @JsonProperty
  public boolean isReportParseExceptions()
  {
    return reportParseExceptions;
  }

  @JsonProperty
  public long getHandoffConditionTimeout()
  {
    return handoffConditionTimeout;
  }

  @JsonProperty
  public boolean isIgnorePreviousSegments()
  {
    return ignorePreviousSegments;
  }

  public RealtimeTuningConfig withVersioningPolicy(VersioningPolicy policy)
  {
    return new RealtimeTuningConfig(
        getMaxRowsInMemory(),
        getMaxOccupationInMemory(),
        intermediatePersistPeriod,
        windowPeriod,
        basePersistDirectory,
        policy,
        rejectionPolicyFactory,
        maxPendingPersists,
        shardSpec,
        getIndexSpec(),
        getBuildV9Directly(),
        persistThreadPriority,
        mergeThreadPriority,
        reportParseExceptions,
        handoffConditionTimeout,
        ignorePreviousSegments
    );
  }

  public RealtimeTuningConfig withBasePersistDirectory(File dir)
  {
    return new RealtimeTuningConfig(
        getMaxRowsInMemory(),
        getMaxOccupationInMemory(),
        intermediatePersistPeriod,
        windowPeriod,
        dir,
        versioningPolicy,
        rejectionPolicyFactory,
        maxPendingPersists,
        shardSpec,
        getIndexSpec(),
        getBuildV9Directly(),
        persistThreadPriority,
        mergeThreadPriority,
        reportParseExceptions,
        handoffConditionTimeout,
        ignorePreviousSegments
    );
  }
}
