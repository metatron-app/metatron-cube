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

package io.druid.emitter.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.Nullable;
import java.util.Map;

public class KafkaEmitterConfig
{

  @JsonProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
  private final String bootstrapServers;
  @JsonProperty("metric.topic")
  private final String metricTopic;
  @JsonProperty("alert.topic")
  private final String alertTopic;
  @JsonProperty("query.topic")
  private final String queryTopic;
  @JsonProperty
  private final String clusterName;
  @JsonProperty("producer.config")
  private Map<String, String> kafkaProducerConfig;

  @JsonCreator
  public KafkaEmitterConfig(
      @JsonProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) String bootstrapServers,
      @JsonProperty("metric.topic") String metricTopic,
      @JsonProperty("alert.topic") String alertTopic,
      @JsonProperty("query.topic") String queryTopic,
      @JsonProperty("clusterName") String clusterName,
      @JsonProperty("producer.config") @Nullable Map<String, String> kafkaProducerConfig
  )
  {
    this.bootstrapServers = Preconditions.checkNotNull(bootstrapServers, "bootstrap.servers can not be null");
    this.metricTopic = Preconditions.checkNotNull(metricTopic, "metric.topic can not be null");
    this.alertTopic = Preconditions.checkNotNull(alertTopic, "alert.topic can not be null");
    this.queryTopic = Preconditions.checkNotNull(queryTopic, "query.topic can not be null");
    this.clusterName = clusterName;
    this.kafkaProducerConfig = kafkaProducerConfig == null ? ImmutableMap.<String, String>of() : kafkaProducerConfig;
  }

  @JsonProperty
  public String getBootstrapServers()
  {
    return bootstrapServers;
  }

  @JsonProperty
  public String getMetricTopic()
  {
    return metricTopic;
  }

  @JsonProperty
  public String getAlertTopic()
  {
    return alertTopic;
  }

  @JsonProperty
  public String getQueryTopic()
  {
    return queryTopic;
  }

  @JsonProperty
  public String getClusterName()
  {
    return clusterName;
  }

  @JsonProperty
  public Map<String, String> getKafkaProducerConfig()
  {
    return kafkaProducerConfig;
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

    KafkaEmitterConfig that = (KafkaEmitterConfig) o;

    if (!getBootstrapServers().equals(that.getBootstrapServers())) {
      return false;
    }
    if (!getMetricTopic().equals(that.getMetricTopic())) {
      return false;
    }
    if (!getAlertTopic().equals(that.getAlertTopic())) {
      return false;
    }
    if (getClusterName() != null ? !getClusterName().equals(that.getClusterName()) : that.getClusterName() != null) {
      return false;
    }
    return getKafkaProducerConfig().equals(that.getKafkaProducerConfig());
  }

  @Override
  public int hashCode()
  {
    int result = getBootstrapServers().hashCode();
    result = 31 * result + getMetricTopic().hashCode();
    result = 31 * result + getAlertTopic().hashCode();
    result = 31 * result + (getClusterName() != null ? getClusterName().hashCode() : 0);
    result = 31 * result + getKafkaProducerConfig().hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "KafkaEmitterConfig{" +
           "bootstrap.servers='" + bootstrapServers + '\'' +
           ", metric.topic='" + metricTopic + '\'' +
           ", alert.topic='" + alertTopic + '\'' +
           ", clusterName='" + clusterName + '\'' +
           ", Producer.config=" + kafkaProducerConfig +
           '}';
  }
}
