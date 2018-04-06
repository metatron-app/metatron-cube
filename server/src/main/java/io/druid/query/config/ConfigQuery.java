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

package io.druid.query.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.common.Intervals;
import io.druid.query.DataSource;
import io.druid.query.FilterableManagementQuery;
import io.druid.query.TableDataSource;
import io.druid.query.jmx.JMXQuery;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

/**
 */
@JsonTypeName("config")
public class ConfigQuery extends JMXQuery implements FilterableManagementQuery
{
  private final Map<String, Map<String, String>> config;

  public ConfigQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("expression") String expression,
      @JsonProperty("config") Map<String, Map<String, String>> config,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource == null ? TableDataSource.of("config") : dataSource,
        querySegmentSpec == null ? new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY) : querySegmentSpec,
        expression,
        context
    );
    this.config = config;
  }

  @Override
  public String getType()
  {
    return "config";
  }

  @Override
  public ConfigQuery withDataSource(DataSource dataSource)
  {
    return new ConfigQuery(
        dataSource,
        getQuerySegmentSpec(),
        getExpression(),
        config,
        getContext()
    );
  }

  @Override
  public ConfigQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new ConfigQuery(
        getDataSource(),
        spec,
        getExpression(),
        config,
        getContext()
    );
  }

  @Override
  public ConfigQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new ConfigQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getExpression(),
        config,
        computeOverridenContext(contextOverride)
    );
  }

  @JsonProperty
  public Map<String, Map<String, String>> getConfig()
  {
    return config;
  }

  @Override
  public String toString()
  {
    return "ConfigQuery{" +
           "expression='" + getExpression() + '\'' +
           ", config=" + config +
           '}';
  }
}
