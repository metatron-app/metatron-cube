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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.DSuppliers.Memoizing;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.query.DataSource;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DruidTable implements TranslatableTable
{
  private final DataSource dataSource;

  private final Memoizing<List<SegmentAnalysis>> supplier;
  private final Memoizing<RowSignature> signature;
  private final Memoizing<Statistic> statistic;
  private final Memoizing<Map<String, Map<String, String>>> descriptors;

  public DruidTable(DataSource dataSource, Memoizing<List<SegmentAnalysis>> supplier)
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.supplier = Preconditions.checkNotNull(supplier, "supplier");
    this.signature = DSuppliers.memoize(
        () -> {
          Set<String> columns = Sets.newHashSet();
          RowSignature.Builder builder = RowSignature.builder();
          for (SegmentAnalysis schema : supplier.get()) {
            for (Map.Entry<String, ColumnAnalysis> entry : schema.getColumns().entrySet()) {
              if (columns.add(entry.getKey())) {
                builder.add(entry.getKey(), ValueDesc.of(entry.getValue().getType()));
              }
            }
          }
          return builder.sort().build();
        }
    );
    this.descriptors = DSuppliers.memoize(
        () -> {
          Map<String, Map<String, String>> builder = Maps.newHashMap();
          for (SegmentAnalysis schema : supplier.get()) {
            for (Map.Entry<String, ColumnAnalysis> entry : schema.getColumns().entrySet()) {
              if (!GuavaUtils.isNullOrEmpty(entry.getValue().getDescriptor())) {
                builder.putIfAbsent(entry.getKey(), entry.getValue().getDescriptor());
              }
            }
          }
          return builder;
        }
    );
    Supplier<Long> rowNum = () -> supplier.get().stream().mapToLong(SegmentAnalysis::getNumRows).sum();
    this.statistic = DSuppliers.memoize(() -> Statistics.of(rowNum.get(), ImmutableList.of()));
  }

  public DruidTable check(long threshold)
  {
    if (supplier.updated() > 0 && supplier.updated() + threshold < System.currentTimeMillis()) {
      supplier.reset();
      signature.reset();
      descriptors.reset();
      statistic.reset();
    }
    return this;
  }

  public DataSource getDataSource()
  {
    return dataSource;
  }

  public RowSignature getRowSignature()
  {
    return signature.get();
  }

  public Map<String, Map<String, String>> getDescriptors()
  {
    return descriptors.get();
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.TABLE;
  }

  @Override
  public Statistic getStatistic()
  {
    return statistic.get();
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
