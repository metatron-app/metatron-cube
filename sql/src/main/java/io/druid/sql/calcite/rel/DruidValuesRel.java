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

package io.druid.sql.calcite.rel;

import io.druid.query.CombinedDataSource;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang.StringUtils;

import java.util.Set;

public class DruidValuesRel extends DruidRel
{
  private final RelNode source;
  private final Iterable<Object[]> values;
  private final String tableName;

  public static DruidValuesRel of(LogicalTableScan source, Iterable<Object[]> values, QueryMaker queryMaker) {
    return new DruidValuesRel(
        source.getCluster(),
        source.getTraitSet(),
        source,
        values,
        StringUtils.join(source.getTable().getQualifiedName(), '.'),
        queryMaker
    );
  }

  public static DruidValuesRel of(RelNode source, Iterable<Object[]> values, QueryMaker queryMaker)
  {
    return new DruidValuesRel(
        source.getCluster(),
        source.getTraitSet(),
        source,
        values,
        null,
        queryMaker
    );
  }

  private DruidValuesRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode source,
      Iterable<Object[]> values,
      String tableName,
      QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.source = source;
    this.values = values;
    this.tableName = tableName;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq, Set<RelNode> visited)
  {
    return planner.getCostFactory().makeCost(0.000001, 0, 0);   // should be less than scan + filter
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    return withFinalize.get();
  }

  @Override
  public DruidQuery makeDruidQuery(boolean finalizeAggregations)
  {
    final RowSignature signature = RowSignature.from(source.getRowType());
    final TypedDummyQuery query = TypedDummyQuery.of(tableName, signature, values)
                                                 .withOverriddenContext(getPlannerContext().copyQueryContext());
    return new DruidQuery()
    {
      @Override
      public RelDataType getOutputRowType()
      {
        return source.getRowType();
      }

      @Override
      public RowSignature getInputRowSignature()
      {
        return signature;
      }

      @Override
      public RowSignature getOutputRowSignature()
      {
        return signature;
      }

      @Override
      public Query getQuery()
      {
        return query;
      }
    };
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return source.getRowType();
  }

  @Override
  public RelNode getLeafRel()
  {
    return source;
  }

  @Override
  public DruidValuesRel asDruidConvention()
  {
    return new DruidValuesRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        source,
        values,
        tableName,
        getQueryMaker()
    );
  }

  @Override
  public DataSource getDataSource()
  {
    return tableName == null ? CombinedDataSource.of() : TableDataSource.of(tableName);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return super.explainTerms(pw)
                .input("source", source)
                .itemIf("table", tableName, tableName != null);
  }
}
