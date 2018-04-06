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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.guava.GuavaUtils;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.RowResolver;
import io.druid.segment.Segment;
import io.druid.server.DruidNode;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class ConfigQueryRunnerFactory implements QueryRunnerFactory<Map<String, Object>, ConfigQuery>
{
  private final DruidNode node;
  private final Injector injector;
  private final ConfigQueryToolChest toolChest;
  private final QueryWatcher queryWatcher;

  @Inject
  public ConfigQueryRunnerFactory(
      @Self DruidNode node,
      Injector injector,
      ConfigQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    this.node = node;
    this.injector = injector;
    this.toolChest = toolChest;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public Future<Object> preFactoring(ConfigQuery query, List<Segment> segments, Supplier<RowResolver> resolver, ExecutorService exec)
  {
    return null;
  }

  @Override
  public QueryRunner<Map<String, Object>> createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new QueryRunner<Map<String, Object>>()
    {
      @Override
      public Sequence<Map<String, Object>> run(Query<Map<String, Object>> query, Map<String, Object> responseContext)
      {
        String hostAndPort = node.getHostAndPort();
        ConfigQuery configQuery = (ConfigQuery)query;
        if (GuavaUtils.isNullOrEmpty(configQuery.getConfig())) {
          Map<String, Object> row = ImmutableMap.<String, Object>of(hostAndPort, JsonConfigProvider.getKeys());
          return Sequences.simple(Arrays.asList(row));
        }
        Map<String, Object> results = Maps.newLinkedHashMap();
        for (Map.Entry<String, Map<String, String>> entry : configQuery.getConfig().entrySet()) {
          String propertyBase = entry.getKey();
          Map<String, String> properties = entry.getValue();
          if (GuavaUtils.isNullOrEmpty(properties)) {
            final Object value = JsonConfigProvider.get(injector, propertyBase);
            results.put(propertyBase, value instanceof Supplier ? ((Supplier)value).get() : value);
          } else {
            results.put(propertyBase, JsonConfigProvider.configure(injector, propertyBase, properties));
          }
        }
        Map<String, Object> row = ImmutableMap.<String, Object>of(hostAndPort, results);
        return Sequences.simple(Arrays.asList(row));
      }
    };
  }

  @Override
  public QueryRunner<Map<String, Object>> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<Map<String, Object>>> queryRunners,
      final Future<Object> optimizer
  )
  {
    return new ChainedExecutionQueryRunner<Map<String, Object>>(queryExecutor, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<Map<String, Object>, ConfigQuery> getToolchest()
  {
    return toolChest;
  }
}
