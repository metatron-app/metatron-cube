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

package io.druid.sql.calcite.table;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.DataSource;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.TableDataSource;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery.AnalysisType;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.timeline.DataSegment;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class DruidTable implements TranslatableTable
{
  private static final Logger LOG = new Logger(DruidTable.class);

  private final TableDataSource dataSource;
  private final QuerySegmentWalker segmentWalker;

  private RowSignature signature;
  private Statistic statistic;
  private Map<String, Map<String, String>> descriptors;

  public DruidTable(TableDataSource dataSource, QuerySegmentWalker segmentWalker)
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.segmentWalker = Preconditions.checkNotNull(segmentWalker, "segmentWalker");
  }

  public void update(DataSegment segment, boolean added)
  {
    Supplier<Holder> update = Suppliers.memoize(
        () -> build(segment.getDataSource(), new SpecificSegmentSpec(segment.toDescriptor()), segmentWalker)
    );
    if (signature != null && !Objects.equals(update.get().signature, signature)) {
      signature = null;
    }
    if (descriptors != null && !Objects.equals(update.get().descriptors, descriptors)) {
      descriptors = null;
    }
    if (statistic != null) {
      final long rowCount = update.get().rowCount;
      statistic = Statistics.of(statistic.getRowCount() + (added ? rowCount : -rowCount), ImmutableList.of());
    }
  }

  public DataSource getDataSource()
  {
    return dataSource;
  }

  public RowSignature getRowSignature()
  {
    if (signature == null) {
      updateFor(signature);
    }
    return signature;
  }

  public Map<String, Map<String, String>> getDescriptors()
  {
    if (descriptors == null) {
      updateFor(descriptors);
    }
    return descriptors;
  }

  @Override
  public Statistic getStatistic()
  {
    if (statistic == null) {
      updateFor(statistic);
    }
    return statistic;
  }

  private synchronized void updateFor(Object needed)
  {
    if (needed == null) {
      long start = System.currentTimeMillis();
      Holder update = build(dataSource.getName(), QuerySegmentSpec.ETERNITY, segmentWalker);
      this.signature = update.signature;
      this.descriptors = update.descriptors;
      this.statistic = Statistics.of(update.rowCount, ImmutableList.of());
      LOG.info("Refreshed schema of [%s].. %,d msec", dataSource.getName(), System.currentTimeMillis() - start);
    }
  }

  private static class Holder
  {
    private final RowSignature signature;
    private final Map<String, Map<String, String>> descriptors;
    private final long rowCount;

    private Holder(RowSignature signature, Map<String, Map<String, String>> descriptors, long rowCount)
    {
      this.signature = signature;
      this.descriptors = descriptors;
      this.rowCount = rowCount;
    }
  }

  private static Holder build(String dataSource, QuerySegmentSpec segmentSpec, QuerySegmentWalker segmentWalker)
  {
    SegmentMetadataQuery query = SegmentMetadataQuery.of(dataSource, segmentSpec, AnalysisType.INTERVAL);
    Sequence<SegmentAnalysis> sequence = QueryRunners.run(query.withId(UUID.randomUUID().toString()), segmentWalker);

    long rowNum = 0;
    Set<String> columns = Sets.newHashSet();
    RowSignature.Builder builder = RowSignature.builder();
    Map<String, Map<String, String>> descriptors = Maps.newHashMap();
    for (SegmentAnalysis schema : Lists.reverse(Sequences.toList(sequence))) {
      for (Map.Entry<String, ColumnAnalysis> entry : schema.getColumns().entrySet()) {
        if (columns.add(entry.getKey())) {
          builder.add(entry.getKey(), ValueDesc.of(entry.getValue().getType()));
          descriptors.put(entry.getKey(), entry.getValue().getDescriptor());
        }
      }
      rowNum += schema.getNumRows();
    }
    return new Holder(builder.sort().build(), descriptors, rowNum);
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.TABLE;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
  {
    return getRowSignature().toRelDataType(typeFactory);
  }

  @Override
  public boolean isRolledUp(String column)
  {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(
      final String column,
      final SqlCall call,
      final SqlNode parent,
      final CalciteConnectionConfig config
  )
  {
    return true;
  }

  @Override
  public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable table)
  {
    return LogicalTableScan.create(context.getCluster(), table);
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

    return Objects.equals(dataSource, ((DruidTable) o).dataSource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(dataSource);
  }
}
