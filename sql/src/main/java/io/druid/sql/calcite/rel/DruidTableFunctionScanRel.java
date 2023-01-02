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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nullable;
import java.util.Set;

public class DruidTableFunctionScanRel extends DruidRel
{
  public static RelNode of(TableFunctionScan scan, QueryMaker queryMaker)
  {
    return new DruidTableFunctionScanRel(
        scan.getCluster(),
        scan.getTraitSet(),
        PartialDruidQuery.create(scan, queryMaker.getPlannerContext()),
        queryMaker
    );
  }

  private final PartialDruidQuery partialQuery;

  private DruidTableFunctionScanRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      PartialDruidQuery partialQuery,
      QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.partialQuery = partialQuery;
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidRel withPartialQuery(PartialDruidQuery partialQuery)
  {
    return new DruidTableFunctionScanRel(
        getCluster(),
        getTraitSet().plus(partialQuery.getCollation()),
        partialQuery,
        queryMaker
    );
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.leafRel().getRowType();
  }

  @Override
  public boolean hasFilter()
  {
    return partialQuery.getScanFilter() != null;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq, Set<RelNode> visited)
  {
    return partialQuery.cost(1, planner.getCostFactory());
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return partialQuery.explainTerms(pw.item("invocation", ((TableFunctionScan) partialQuery.getScan()).getCall()));
  }

  @Nullable
  @Override
  protected DruidQuery makeDruidQuery(boolean finalizeAggregations)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DruidRel asDruidConvention()
  {
    return new DruidTableFunctionScanRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        partialQuery,
        queryMaker
    );
  }

  @Override
  public DataSource getDataSource()
  {
    return CombinedDataSource.of();
  }
}
