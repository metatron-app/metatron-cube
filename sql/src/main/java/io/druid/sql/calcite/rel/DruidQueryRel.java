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

import com.google.common.base.Preconditions;
import io.druid.query.DataSource;
import io.druid.sql.calcite.table.DruidTable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * DruidRel that uses a "table" dataSource.
 */
public class DruidQueryRel extends DruidRel
{
  private final RelOptTable table;
  private final DruidTable druidTable;
  private final PartialDruidQuery partialQuery;

  private DruidQueryRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelOptTable table,
      final DruidTable druidTable,
      final QueryMaker queryMaker,
      final PartialDruidQuery partialQuery
  )
  {
    super(cluster, traitSet, queryMaker);
    this.table = Preconditions.checkNotNull(table, "table");
    this.druidTable = Preconditions.checkNotNull(druidTable, "druidTable");
    this.partialQuery = Preconditions.checkNotNull(partialQuery, "partialQuery");
  }

  /**
   * Create a DruidQueryRel representing a full scan.
   */
  public static DruidQueryRel fullScan(
      final LogicalTableScan scanRel,
      final RelOptTable table,
      final DruidTable druidTable,
      final QueryMaker queryMaker
  )
  {
    return new DruidQueryRel(
        scanRel.getCluster(),
        scanRel.getCluster().traitSetOf(Convention.NONE),
        table,
        druidTable,
        queryMaker,
        PartialDruidQuery.create(scanRel, queryMaker.getPlannerContext())
    );
  }

  @Override
  @Nonnull
  public DruidQuery makeDruidQuery(final boolean finalizeAggregations)
  {
    return partialQuery.build(
        druidTable.getDataSource(),
        druidTable.getRowSignature(),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations
    );
  }

  @Override
  public DruidQueryRel asDruidConvention()
  {
    return new DruidQueryRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        table,
        druidTable,
        getQueryMaker(),
        partialQuery
    );
  }

  @Override
  public DataSource getDataSource()
  {
    return druidTable.getDataSource();
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidQueryRel withPartialQuery(final PartialDruidQuery newQueryBuilder)
  {
    return new DruidQueryRel(
        getCluster(),
        getTraitSet().plus(newQueryBuilder.getCollation()),
        table,
        druidTable,
        getQueryMaker(),
        newQueryBuilder
    );
  }

  @Override
  public Filter getFilter()
  {
    return partialQuery.getScanFilter();
  }

  @Override
  public RelOptTable getTable()
  {
    return table;
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return partialQuery.explainTerms(pw.item("table", StringUtils.join(table.getQualifiedName(), '.')));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq, Set<RelNode> visited)
  {
    return planner.getCostFactory().makeCost(partialQuery.cost(druidTable), 1, 0);
  }
}
