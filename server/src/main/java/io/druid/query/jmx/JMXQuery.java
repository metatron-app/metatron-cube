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

package io.druid.query.jmx;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import io.druid.client.DruidServer;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.common.Intervals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.FilterableManagementQuery;
import io.druid.query.TableDataSource;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("jmx")
public class JMXQuery extends BaseQuery<Map<String, Object>> implements FilterableManagementQuery
{
  private final String expression;

  public JMXQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("expression") String expression,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource == null ? TableDataSource.of("jmx") : dataSource,
        querySegmentSpec == null ? new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY) : querySegmentSpec,
        false,
        context
    );
    this.expression = expression;
  }

  @Override
  public String getType()
  {
    return "jmx";
  }

  @Override
  public JMXQuery withDataSource(DataSource dataSource)
  {
    return new JMXQuery(
        dataSource,
        getQuerySegmentSpec(),
        expression,
        getContext()
    );
  }

  @Override
  public JMXQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new JMXQuery(
        getDataSource(),
        spec,
        expression,
        getContext()
    );
  }

  @Override
  public JMXQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new JMXQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        expression,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public String toString()
  {
    return "JMXQuery{" +
           "expression='" + expression + '\'' +
           '}';
  }

  @Override
  public List<QueryableDruidServer> filter(List<QueryableDruidServer> servers)
  {
    if (expression == null) {
      return servers;
    }
    Expr expr = Parser.parse(expression);

    List<QueryableDruidServer> passed = Lists.newArrayList();
    for (QueryableDruidServer server : servers) {
      DruidServer druidServer = server.getServer();
      HostAndPort hostAndPort = HostAndPort.fromString(druidServer.getName());
      Expr.NumericBinding bindings = Parser.withMap(
          ImmutableMap.<String, Object>of(
              "name", druidServer.getName(),
              "type", druidServer.getType(),
              "tier", druidServer.getTier(),
              "host", hostAndPort.getHostText(),
              "port", hostAndPort.getPort())
      );
      if (expr.eval(bindings).asBoolean()) {
        passed.add(server);
      }
    }
    return passed;
  }
}
