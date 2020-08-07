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

package io.druid.query.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import io.druid.client.DruidServer;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.FilterableManagementQuery;
import io.druid.query.TableDataSource;
import io.druid.query.jmx.JMXQuery;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeName("config")
public class ConfigQuery extends BaseQuery<Map<String, Object>> implements FilterableManagementQuery
{
  private final String expression;
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
        querySegmentSpec,
        false,
        context
    );
    this.expression = expression;
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
        expression,
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
        expression,
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
        expression,
        config,
        computeOverriddenContext(contextOverride)
    );
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty
  public Map<String, Map<String, String>> getConfig()
  {
    return config;
  }

  @Override
  public Ordering<Map<String, Object>> getMergeOrdering()
  {
    return null;
  }

  @Override
  public List<DruidServer> filter(List<DruidServer> servers)
  {
    return JMXQuery.filterServers(expression, servers);
  }

  @Override
  public String toString()
  {
    return "ConfigQuery{" +
           "expression='" + expression + '\'' +
           ", config=" + config +
           '}';
  }

  @Override
  public Set<String> supports()
  {
    return ImmutableSet.of();
  }
}
