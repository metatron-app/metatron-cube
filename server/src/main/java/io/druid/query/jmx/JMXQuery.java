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

package io.druid.query.jmx;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.druid.client.DruidServer;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.HostAndPort;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.FilterableManagementQuery;
import io.druid.query.TableDataSource;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.druid.server.ServiceTypes.BROKER;
import static io.druid.server.ServiceTypes.COORDINATOR;
import static io.druid.server.ServiceTypes.HISTORICAL;
import static io.druid.server.ServiceTypes.MIDDLE_MANAGER;
import static io.druid.server.ServiceTypes.OVERLORD;

/**
 */
@JsonTypeName("jmx")
public class JMXQuery extends BaseQuery<Map<String, Object>> implements FilterableManagementQuery
{
  public static JMXQuery of(String expression)
  {
    return new JMXQuery(null, null, expression, false, null);
  }

  private final String expression;
  private final boolean dumpLongestStack;

  public JMXQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("expression") String expression,
      @JsonProperty("dumpLongestStack") boolean dumpLongestStack,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource == null ? TableDataSource.of("jmx") : dataSource,
        querySegmentSpec,
        false,
        context
    );
    this.expression = expression;
    this.dumpLongestStack = dumpLongestStack;
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
        dumpLongestStack,
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
        dumpLongestStack,
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
        dumpLongestStack,
        computeOverriddenContext(contextOverride)
    );
  }

  public JMXQuery withExpression(String expression)
  {
    return new JMXQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        expression,
        dumpLongestStack,
        getContext()
    );
  }

  @Override
  public Ordering<Map<String, Object>> getMergeOrdering()
  {
    return GuavaUtils.nullFirstNatural().onResultOf(
        new Function<Map<String, Object>, Comparable>()
        {
          @Override
          public Comparable apply(Map<String, Object> input)
          {
            return Iterables.getFirst(input.keySet(), null);
          }
        }
    );
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty
  public boolean isDumpLongestStack()
  {
    return dumpLongestStack;
  }

  @Override
  public String toString()
  {
    return "JMXQuery{" +
           "expression='" + expression + '\'' +
           '}';
  }

  @Override
  public List<DruidServer> filter(List<DruidServer> servers)
  {
    return filterServers(expression, servers);
  }

  public static List<DruidServer> filterServers(String expression, List<DruidServer> servers)
  {
    if (expression == null) {
      return servers;
    }
    Expr expr = Parser.parse(expression);

    List<DruidServer> passed = Lists.newArrayList();
    for (DruidServer server : servers) {
      HostAndPort hostAndPort = HostAndPort.fromString(server.getName());
      Expr.NumericBinding bindings = Parser.withMap(
          ImmutableMap.<String, Object>of(
              "name", server.getName(),
              "type", server.getType(),
              "tier", server.getTier(),
              "host", hostAndPort.getHostText(),
              "port", hostAndPort.getPort()
          )
      );
      if (expr.eval(bindings).asBoolean()) {
        passed.add(server);
      }
    }
    return passed;
  }

  @Override
  public Set<String> supports()
  {
    return ImmutableSet.of(COORDINATOR, BROKER, HISTORICAL, OVERLORD, MIDDLE_MANAGER);
  }
}
