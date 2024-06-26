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

package io.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexing.kafka.KafkaTuningConfig;
import io.druid.segment.IndexSpec;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.io.File;

public class KafkaSupervisorTuningConfig extends KafkaTuningConfig
{
  private static final Duration DEFAULT_HTTTP_TIMEOUT = new Period("PT10S").toStandardDuration();
  private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = new Period("PT80S").toStandardDuration();
  private static final Duration DEFAULT_OFFSETFETCH_PERIOD = new Period("PT30S").toStandardDuration();

  private final Integer workerThreads;
  private final Integer chatThreads;
  private final Long chatRetries;
  private final Duration httpTimeout;
  private final Duration shutdownTimeout;
  private final Duration offsetFetchPeriod;

  public KafkaSupervisorTuningConfig(
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
      @JsonProperty("resetOffsetAutomatically") Boolean resetOffsetAutomatically,
      @JsonProperty("workerThreads") Integer workerThreads,
      @JsonProperty("chatThreads") Integer chatThreads,
      @JsonProperty("chatRetries") Long chatRetries,
      @JsonProperty("httpTimeout") Period httpTimeout,
      @JsonProperty("shutdownTimeout") Period shutdownTimeout,
      @JsonProperty("offsetFetchPeriod") Period offsetFetchPeriod
  )
  {
    super(
        maxRowsInMemory,
        maxOccupationInMemory,
        maxRowsPerSegment,
        intermediatePersistPeriod,
        basePersistDirectory,
        maxPendingPersists,
        indexSpec,
        buildV9Directly,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically
    );

    this.workerThreads = workerThreads;
    this.chatThreads = chatThreads;
    this.chatRetries = chatRetries != null ? chatRetries : 8;
    this.httpTimeout = httpTimeout == null ? DEFAULT_HTTTP_TIMEOUT : httpTimeout.toStandardDuration();
    this.shutdownTimeout = shutdownTimeout == null ? DEFAULT_SHUTDOWN_TIMEOUT : shutdownTimeout.toStandardDuration();
    this.offsetFetchPeriod = offsetFetchPeriod == null ? DEFAULT_OFFSETFETCH_PERIOD : offsetFetchPeriod.toStandardDuration();
  }

  @JsonProperty
  public Integer getWorkerThreads()
  {
    return workerThreads;
  }

  @JsonProperty
  public Integer getChatThreads()
  {
    return chatThreads;
  }

  @JsonProperty
  public Long getChatRetries()
  {
    return chatRetries;
  }

  @JsonProperty
  public Duration getHttpTimeout()
  {
    return httpTimeout;
  }

  @JsonProperty
  public Duration getShutdownTimeout()
  {
    return shutdownTimeout;
  }

  @JsonProperty
  public Duration getOffsetFetchPeriod()
  {
    return offsetFetchPeriod;
  }

  @Override
  public String toString()
  {
    return "KafkaSupervisorTuningConfig{" +
           "maxRowsInMemory=" + getMaxRowsInMemory() +
           ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
           ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
           ", basePersistDirectory=" + getBasePersistDirectory() +
           (isReportParseExceptions() ? ", reportParseExceptions=true" : "") +
           (isResetOffsetAutomatically() ? ", resetOffsetAutomatically=true" : "") +
           (getHandoffConditionTimeout() > 0 ? ", handoffConditionTimeout=" + getHandoffConditionTimeout() : "") +
           (workerThreads != null ? ", workerThreads=" + workerThreads : "") +
           (chatThreads != null ? ", chatThreads=" + chatThreads : "") +
           ", chatRetries=" + chatRetries +
           (httpTimeout != DEFAULT_HTTTP_TIMEOUT ? ", httpTimeout=" + httpTimeout : "") +
           (shutdownTimeout != DEFAULT_SHUTDOWN_TIMEOUT ? ", shutdownTimeout=" + shutdownTimeout : "") +
           (offsetFetchPeriod != DEFAULT_OFFSETFETCH_PERIOD ? ", offsetFetchPeriod=" + offsetFetchPeriod : "") +
           '}';
  }
}
