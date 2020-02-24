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

import io.druid.query.Query;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;
import java.util.List;

public class DruidValuesRel extends DruidRel<DruidValuesRel>
{
  private final RelNode source;
  private final Iterable<Object[]> values;

  public DruidValuesRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode source,
      Iterable<Object[]> values,
      QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.source = source;
    this.values = values;
  }

  @Override
  public int getQueryCount()
  {
    return 0;
  }

  @Override
  public DruidValuesRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return null;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeTinyCost();
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    return new DruidQuery()
    {
      @Override
      public RelDataType getOutputRowType()
      {
        return source.getRowType();
      }

      @Override
      public RowSignature getOutputRowSignature()
      {
        return RowSignature.from(source.getRowType());
      }

      @Override
      public Query getQuery()
      {
        return TypedDummyQuery.of(RowSignature.from(source.getRowType()), values)
                              .withOverriddenContext(getPlannerContext().getQueryContext());
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
  public DruidQuery toDruidQueryForExplaining()
  {
    return toDruidQuery(false);
  }

  @Override
  public DruidValuesRel asDruidConvention()
  {
    return new DruidValuesRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        source,
        values,
        getQueryMaker()
    );
  }

  @Override
  public List<String> getDataSourceNames()
  {
    return Arrays.asList();
  }
}
