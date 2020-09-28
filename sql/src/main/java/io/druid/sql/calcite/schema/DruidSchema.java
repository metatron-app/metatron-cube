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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.client.ServerView;
import io.druid.client.TimelineServerView;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.ValueDesc;
import io.druid.guice.ManageLifecycle;
import io.druid.query.Query;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.TableDataSource;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery.AnalysisType;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.sql.calcite.table.DruidTable.WithTimestamp;
import io.druid.sql.calcite.table.RowSignature;
import io.druid.sql.calcite.view.DruidViewMacro;
import io.druid.sql.calcite.view.ViewManager;
import io.druid.timeline.DataSegment;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

@ManageLifecycle
public class DruidSchema extends AbstractSchema
    implements BiFunction<String, WithTimestamp, WithTimestamp>
{
  public static final String NAME = "druid";
  public static final long CACHE_VALID_MSEC = 300_000;   // 5 min

  private final QuerySegmentWalker segmentWalker;
  private final TimelineServerView serverView;
  private final ViewManager viewManager;
  private final Map<String, WithTimestamp> cached;

  @Inject
  public DruidSchema(
      final @JacksonInject QuerySegmentWalker segmentWalker,
      final TimelineServerView serverView,
      final ViewManager viewManager
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
            cached.remove(segment.getDataSource());
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DruidServerMetadata server, final DataSegment segment)
          {
            cached.remove(segment.getDataSource());
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  @Override
  protected Map<String, Table> getTableMap()
  {
    return new AbstractMap<String, Table>()
    {
      @Override
      public Set<String> keySet()
      {
        return ImmutableSet.copyOf(serverView.getDataSources());
      }

      @Override
      public Table get(Object key)
      {
        return cached.compute((String) key, DruidSchema.this);
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
  public WithTimestamp apply(String tableName, WithTimestamp prev)
  {
    if (prev != null && prev.getTimestamp() + CACHE_VALID_MSEC > System.currentTimeMillis()) {
      return prev;
    }
    if (serverView.getTimeline(tableName) == null) {
      return null;
    }
    Query<SegmentAnalysis> metaQuery = SegmentMetadataQuery.of(tableName, AnalysisType.INTERVAL)
                                                           .withId(UUID.randomUUID().toString());
    List<SegmentAnalysis> schemas = Sequences.toList(QueryRunners.run(metaQuery, segmentWalker));

    long numRows = 0;
    Set<String> columns = Sets.newHashSet();
    Map<String, Map<String, String>> descriptors = Maps.newHashMap();
    RowSignature.Builder builder = RowSignature.builder();
    for (SegmentAnalysis schema : Lists.reverse(schemas)) {
      for (Map.Entry<String, ColumnAnalysis> entry : schema.getColumns().entrySet()) {
        if (columns.add(entry.getKey())) {
          builder.add(entry.getKey(), ValueDesc.of(entry.getValue().getType()));
          if (!GuavaUtils.isNullOrEmpty(entry.getValue().getDescriptor())) {
            descriptors.put(entry.getKey(), entry.getValue().getDescriptor());
          }
        }
      }
      numRows += schema.getNumRows();
    }
    return new WithTimestamp(TableDataSource.of(tableName), builder.sort().build(), descriptors, numRows);
  }
}
