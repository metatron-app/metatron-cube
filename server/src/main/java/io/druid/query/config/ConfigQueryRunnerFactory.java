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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.segment.Segment;
import io.druid.server.DruidNode;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;

/**
 */
public class ConfigQueryRunnerFactory extends QueryRunnerFactory.Abstract<Map<String, Object>, ConfigQuery>
{
  private final DruidNode node;
  private final Injector injector;

  @Inject
  public ConfigQueryRunnerFactory(
      @Self DruidNode node,
      Injector injector,
      ConfigQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
    this.node = node;
    this.injector = injector;
  }

  @Override
  public QueryRunner<Map<String, Object>> _createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new QueryRunner<Map<String, Object>>()
    {
      @Override
      public Sequence<Map<String, Object>> run(Query<Map<String, Object>> query, Map<String, Object> responseContext)
      {
        String hostAndPort = node.getHostAndPort();
        ConfigQuery configQuery = (ConfigQuery) query;
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
}
