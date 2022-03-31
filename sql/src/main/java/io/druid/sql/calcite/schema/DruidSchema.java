/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.sql.calcite.schema;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.client.ServerView;
import io.druid.client.TimelineServerView;
import io.druid.common.guava.GuavaUtils;
import io.druid.concurrent.Execs;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.TableDataSource;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.view.DruidViewMacro;
import io.druid.sql.calcite.view.ViewManager;
import io.druid.timeline.DataSegment;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

@ManageLifecycle
public class DruidSchema extends AbstractSchema implements BiFunction<String, DruidTable, DruidTable>
{
  private static final Logger LOG = new Logger(DruidSchema.class);

  public static final String NAME = "druid";

  private final QuerySegmentWalker segmentWalker;
  private final TimelineServerView serverView;
  private final ViewManager viewManager;
  private final Map<String, DruidTable> cached;
  private final Map<String, String> multitenants;

  @Inject
  public DruidSchema(
      final @JacksonInject QuerySegmentWalker segmentWalker,
      final TimelineServerView serverView,
      final ViewManager viewManager,
      final @MultiTenants Map<String, String> multitenants
  )
  {
    this.segmentWalker = Preconditions.checkNotNull(segmentWalker, "segmentWalker");
    this.serverView = Preconditions.checkNotNull(serverView, "serverView");
    this.viewManager = Preconditions.checkNotNull(viewManager, "viewManager");
    this.cached = Maps.newConcurrentMap();
    serverView.registerTimelineCallback(
        Execs.singleThreaded("schema-callback"),
        new TimelineServerView.TimelineCallback()
        {
          @Override
          public ServerView.CallbackAction timelineInitialized()
          {
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentAdded(final DruidServerMetadata server, final DataSegment segment)
          {
            DruidTable table = cached.get(segment.getDataSource());
            if (table != null) {
              table.update(segment, true);
            }
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DruidServerMetadata server, final DataSegment segment)
          {
            DruidTable table = cached.get(segment.getDataSource());
            if (table != null) {
              table.update(segment, false);
            }
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
    this.multitenants = multitenants == null ? ImmutableMap.of() : multitenants;
    if (!GuavaUtils.isNullOrEmpty(multitenants)) {
      for (Map.Entry<String, String> m : multitenants.entrySet()) {
        LOG.info("Table [%s] is registerd with tenant column '%s'", m.getKey(), m.getValue());
      }
    }
  }

  @Override
  protected Map<String, Table> getTableMap()
  {
    return new AbstractMap<String, Table>()
    {
      @Override
      public Set<String> keySet()
      {
        if (multitenants.isEmpty()) {
          return ImmutableSet.copyOf(serverView.getDataSources());
        }
        Set<String> datasources = Sets.newHashSet(serverView.getDataSources());
        for (String multitenant : multitenants.keySet()) {
          DruidTable source = cached.compute(multitenant, DruidSchema.this);
          if (source != null && source.isMultiTenent()) {
            String prefix = source.getDataSource().getName() + "_";
            Iterables.addAll(datasources, Iterables.transform(source.getTenants(), t -> prefix + t));
          }
        }
        return datasources;
      }

      @Override
      public Table get(Object key)
      {
        String tableName = (String) key;
        if (serverView.getTimeline(tableName) == null) {
          int index = tableName.indexOf('_');
          for (; index > 0 && index < tableName.length() - 1; index = tableName.indexOf('_', index + 1)) {
            String sourceName = tableName.substring(0, index);
            DruidTable source = cached.compute(sourceName, DruidSchema.this);
            if (source != null && source.isMultiTenent()) {
              return new DruidTable.Tenant(source, tableName.substring(index + 1), segmentWalker);
            }
          }
        }
        return cached.compute(tableName, DruidSchema.this);
      }

      @Override
      public Set<Entry<String, Table>> entrySet()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  protected Multimap<String, org.apache.calcite.schema.Function> getFunctionMultimap()
  {
    final ImmutableMultimap.Builder<String, org.apache.calcite.schema.Function> builder = ImmutableMultimap.builder();
    for (Map.Entry<String, DruidViewMacro> entry : viewManager.getViews().entrySet()) {
      builder.put(entry);
    }
    return builder.build();
  }

  @Override
  public DruidTable apply(String tableName, DruidTable table)
  {
    if (serverView.getTimeline(tableName) == null) {
      table = null;
    } else if (table == null) {
      table = new DruidTable(TableDataSource.of(tableName), segmentWalker, multitenants.get(tableName));
    }
    return table;
  }
}
