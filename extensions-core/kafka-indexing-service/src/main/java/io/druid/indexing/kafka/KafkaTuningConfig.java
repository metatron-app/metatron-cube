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

package io.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.segment.IndexSpec;
import io.druid.segment.incremental.BaseTuningConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.joda.time.Period;

import java.io.File;
import java.util.Objects;

@JsonTypeName("kafka")
public class KafkaTuningConfig extends BaseTuningConfig implements AppenderatorConfig
{
  private static final int DEFAULT_MAX_ROWS_PER_SEGMENT = 5_000_000;
  private static final boolean DEFAULT_RESET_OFFSET_AUTOMATICALLY = false;

  private final int maxRowsPerSegment;
  private final Period intermediatePersistPeriod;
  private final File basePersistDirectory;
  private final int maxPendingPersists;
  private final boolean reportParseExceptions;
  private final long handoffConditionTimeout;
  private final boolean resetOffsetAutomatically;

  @JsonCreator
  public KafkaTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxOccupationInMemory") Long maxOccupationInMemory,
      @JsonProperty("maxRowsPerSegment") Integer maxRowsPerSegment,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout,
      @JsonProperty("resetOffsetAutomatically") Boolean resetOffsetAutomatically
  )
  {
    super(indexSpec, maxRowsInMemory, maxOccupationInMemory, buildV9Directly, false);
    // Cannot be a static because default basePersistDirectory is unique per-instance
    final RealtimeTuningConfig defaults = RealtimeTuningConfig.makeDefaultTuningConfig(basePersistDirectory);

    this.maxRowsPerSegment = maxRowsPerSegment == null ? DEFAULT_MAX_ROWS_PER_SEGMENT : maxRowsPerSegment;
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? defaults.getIntermediatePersistPeriod()
                                     : intermediatePersistPeriod;
    this.basePersistDirectory = defaults.getBasePersistDirectory();
    this.maxPendingPersists = maxPendingPersists == null ? defaults.getMaxPendingPersists() : maxPendingPersists;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? defaults.isReportParseExceptions()
                                 : reportParseExceptions;
    this.handoffConditionTimeout = handoffConditionTimeout == null
                                   ? defaults.getHandoffConditionTimeout()
                                   : handoffConditionTimeout;
    this.resetOffsetAutomatically = resetOffsetAutomatically == null
                                    ? DEFAULT_RESET_OFFSET_AUTOMATICALLY
                                    : resetOffsetAutomatically;
  }

  public static KafkaTuningConfig copyOf(KafkaTuningConfig config)
  {
    return new KafkaTuningConfig(
        config.getMaxRowsInMemory(),
        config.getMaxOccupationInMemory(),
        config.maxRowsPerSegment,
        config.intermediatePersistPeriod,
        config.basePersistDirectory,
        config.maxPendingPersists,
        config.getIndexSpec(),
        config.getBuildV9Directly(),
        config.reportParseExceptions,
        config.handoffConditionTimeout,
        config.resetOffsetAutomatically
    );
  }

  @JsonProperty
  public int getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }

  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }

  @JsonProperty
  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

  @JsonProperty
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
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
  public boolean isResetOffsetAutomatically()
  {
    return resetOffsetAutomatically;
  }

  public KafkaTuningConfig withBasePersistDirectory(File dir)
  {
    return new KafkaTuningConfig(
        getMaxRowsInMemory(),
        getMaxOccupationInMemory(),
        maxRowsPerSegment,
        intermediatePersistPeriod,
        dir,
        maxPendingPersists,
        getIndexSpec(),
        getBuildV9Directly(),
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically
    );
  }

  public KafkaTuningConfig withMaxRowsInMemory(int rows)
  {
    return new KafkaTuningConfig(
        rows,
        getMaxOccupationInMemory(),
        maxRowsPerSegment,
        intermediatePersistPeriod,
        basePersistDirectory,
        maxPendingPersists,
        getIndexSpec(),
        getBuildV9Directly(),
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KafkaTuningConfig that = (KafkaTuningConfig) o;

    if (getMaxRowsInMemory() != that.getMaxRowsInMemory()) {
      return false;
    }
    if (getMaxOccupationInMemory() != that.getMaxOccupationInMemory()) {
      return false;
    }
    if (maxRowsPerSegment != that.maxRowsPerSegment) {
      return false;
    }
    if (maxPendingPersists != that.maxPendingPersists) {
      return false;
    }
    if (getBuildV9Directly() != that.getBuildV9Directly()) {
      return false;
    }
    if (reportParseExceptions != that.reportParseExceptions) {
      return false;
    }
    if (handoffConditionTimeout != that.handoffConditionTimeout) {
      return false;
    }
    if (resetOffsetAutomatically != that.resetOffsetAutomatically) {
      return false;
    }
    if (intermediatePersistPeriod != null
        ? !intermediatePersistPeriod.equals(that.intermediatePersistPeriod)
        : that.intermediatePersistPeriod != null) {
      return false;
    }
    if (basePersistDirectory != null
        ? !basePersistDirectory.equals(that.basePersistDirectory)
        : that.basePersistDirectory != null) {
      return false;
    }
    return Objects.equals(getIndexSpec(), that.getIndexSpec());
  }
}
