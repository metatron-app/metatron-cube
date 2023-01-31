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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.util.Map;

public class KafkaSupervisorIOConfig
{
  public static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";

  private static final Duration DEFAULT_TASK_DURATION = new Period("PT1H").toStandardDuration();
  private static final Duration DEFAULT_START_DELAY = new Period("PT5S").toStandardDuration();
  private static final Duration DEFAULT_PERIOD = new Period("PT30S").toStandardDuration();
  private static final Duration DEFAULT_COMPLETION_TIMEOUT = new Period("PT30M").toStandardDuration();

  private final String topic;
  private final Integer replicas;
  private final Integer taskCount;
  private final Duration taskDuration;
  private final Map<String, String> consumerProperties;
  private final Duration startDelay;
  private final Duration period;
  private final boolean useEarliestOffset;
  private final Duration completionTimeout;
  private final Optional<Duration> lateMessageRejectionPeriod;
  private final Optional<Duration> earlyMessageRejectionPeriod;
  private final boolean skipOffsetGaps;

  @JsonCreator
  public KafkaSupervisorIOConfig(
      @JsonProperty("topic") String topic,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("taskCount") Integer taskCount,
      @JsonProperty("taskDuration") Period taskDuration,
      @JsonProperty("consumerProperties") Map<String, String> consumerProperties,
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period,
      @JsonProperty("useEarliestOffset") Boolean useEarliestOffset,
      @JsonProperty("completionTimeout") Period completionTimeout,
      @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
      @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod,
      @JsonProperty("skipOffsetGaps") Boolean skipOffsetGaps
  )
  {
    this.topic = Preconditions.checkNotNull(topic, "topic");
    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    Preconditions.checkNotNull(
        consumerProperties.get(BOOTSTRAP_SERVERS_KEY),
        String.format("consumerProperties must contain entry for [%s]", BOOTSTRAP_SERVERS_KEY)
    );

    this.replicas = replicas != null ? replicas : 1;
    this.taskCount = taskCount != null ? taskCount : 1;
    this.taskDuration = taskDuration == null ? DEFAULT_TASK_DURATION : taskDuration.toStandardDuration();
    this.startDelay = startDelay == null ? DEFAULT_START_DELAY : startDelay.toStandardDuration();
    this.period = period == null ? DEFAULT_PERIOD : period.toStandardDuration();
    this.completionTimeout = completionTimeout == null ? DEFAULT_COMPLETION_TIMEOUT : completionTimeout.toStandardDuration();
    this.useEarliestOffset = useEarliestOffset != null ? useEarliestOffset : false;
    this.lateMessageRejectionPeriod = lateMessageRejectionPeriod == null
                                      ? Optional.<Duration>absent()
                                      : Optional.of(lateMessageRejectionPeriod.toStandardDuration());
    this.earlyMessageRejectionPeriod = earlyMessageRejectionPeriod == null
                                       ? Optional.<Duration>absent()
                                       : Optional.of(earlyMessageRejectionPeriod.toStandardDuration());
    this.skipOffsetGaps = skipOffsetGaps != null ? skipOffsetGaps : false;
  }

  @JsonProperty
  public String getTopic()
  {
    return topic;
  }

  @JsonProperty
  public Integer getReplicas()
  {
    return replicas;
  }

  @JsonProperty
  public Integer getTaskCount()
  {
    return taskCount;
  }

  @JsonProperty
  public Duration getTaskDuration()
  {
    return taskDuration;
  }

  @JsonProperty
  public Map<String, String> getConsumerProperties()
  {
    return consumerProperties;
  }

  @JsonProperty
  public Duration getStartDelay()
  {
    return startDelay;
  }

  @JsonProperty
  public Duration getPeriod()
  {
    return period;
  }

  @JsonProperty
  public boolean isUseEarliestOffset()
  {
    return useEarliestOffset;
  }

  @JsonProperty
  public Duration getCompletionTimeout()
  {
    return completionTimeout;
  }

  @JsonProperty
  public Optional<Duration> getEarlyMessageRejectionPeriod()
  {
    return earlyMessageRejectionPeriod;
  }

  @JsonProperty
  public Optional<Duration> getLateMessageRejectionPeriod()
  {
    return lateMessageRejectionPeriod;
  }

  @JsonProperty
  public boolean isSkipOffsetGaps()
  {
    return skipOffsetGaps;
  }

  @Override
  public String toString()
  {
    return "KafkaSupervisorIOConfig{" +
           "topic='" + topic + '\'' +
           ", replicas=" + replicas +
           ", taskCount=" + taskCount +
           ", taskDuration=" + taskDuration +
           ", consumerProperties=" + consumerProperties +
           ", startDelay=" + startDelay +
           ", period=" + period +
           ", useEarliestOffset=" + useEarliestOffset +
           ", completionTimeout=" + completionTimeout +
           (earlyMessageRejectionPeriod.isPresent() ? ", earlyMessageRejectionPeriod=" + earlyMessageRejectionPeriod : "") +
           (lateMessageRejectionPeriod.isPresent() ? ", lateMessageRejectionPeriod=" + lateMessageRejectionPeriod : "") +
           ", skipOffsetGaps=" + skipOffsetGaps +
           '}';
  }
}
