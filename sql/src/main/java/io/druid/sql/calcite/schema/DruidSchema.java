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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.client.TimelineServerView;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.Pair;
import io.druid.query.Query;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.TableDataSource;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery.AnalysisType;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.table.RowSignature;
import io.druid.sql.calcite.view.DruidViewMacro;
import io.druid.sql.calcite.view.ViewManager;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ManageLifecycle
public class DruidSchema extends AbstractSchema
{
  public static final String NAME = "druid";

  private final QuerySegmentWalker segmentWalker;
  private final TimelineServerView serverView;
  private final ViewManager viewManager;

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
        final TableDataSource dataSource = TableDataSource.of((String) key);
        if (serverView.getTimeline(dataSource) == null) {
          return null;
        }
        Supplier<Pair<RowSignature, Long>> supplier = Suppliers.memoize(new Supplier<Pair<RowSignature, Long>>()
        {
          @Override
          public Pair<RowSignature, Long> get()
          {
            Query<SegmentAnalysis> metaQuery = SegmentMetadataQuery.of(dataSource.getName(), AnalysisType.INTERVAL)
                                                                   .withId(UUID.randomUUID().toString());
            List<SegmentAnalysis> schemas = Sequences.toList(QueryRunners.run(metaQuery, segmentWalker));

            long numRows = 0;
            Set<String> columns = Sets.newHashSet();
            RowSignature.Builder builder = RowSignature.builder();
            for (SegmentAnalysis schema : Lists.reverse(schemas)) {
              for (Map.Entry<String, ColumnAnalysis> entry : schema.getColumns().entrySet()) {
                if (columns.add(entry.getKey())) {
                  builder.add(entry.getKey(), ValueDesc.of(entry.getValue().getType()));
                }
              }
              numRows += schema.getNumRows();
            }
            return Pair.of(builder.sort().build(), numRows);
          }
        });
        return new DruidTable(dataSource, supplier);
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
}
