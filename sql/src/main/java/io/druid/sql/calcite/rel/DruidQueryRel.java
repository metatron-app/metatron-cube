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
import com.google.common.collect.ImmutableMap;
import io.druid.query.DataSource;
import io.druid.query.Estimations;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.planner.DruidMetadataProvider;
import io.druid.sql.calcite.table.DruidTable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

/**
 * DruidRel that uses a "table" dataSource.
 */
public class DruidQueryRel extends DruidRel
{
  private final RelOptTable table;
  private final DruidTable druidTable;
  private final PartialDruidQuery partialQuery;
  private final Map<String, Object> contextOverride;

  private DruidQueryRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelOptTable table,
      final DruidTable druidTable,
      final PartialDruidQuery partialQuery,
      final Map<String, Object> contextOverride,
      final QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.table = Preconditions.checkNotNull(table, "table");
    this.druidTable = Preconditions.checkNotNull(druidTable, "druidTable");
    this.contextOverride = contextOverride;
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
        PartialDruidQuery.create(scanRel, queryMaker.getPlannerContext()),
        ImmutableMap.of(),
        queryMaker
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
        contextOverride,
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
        partialQuery,
        contextOverride,
        queryMaker
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
        newQueryBuilder,
        contextOverride,
        queryMaker
    );
  }

  public DruidQueryRel withContextOverride(Map<String, Object> contextOverride)
  {
    return new DruidQueryRel(
        getCluster(),
        getTraitSet(),
        table,
        druidTable,
        partialQuery,
        contextOverride,
        queryMaker
    );
  }

  @Override
  public boolean hasFilter()
  {
    return partialQuery.getScanFilter() != null;
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

  public DruidQueryRel propagateEstimates(RelMetadataQuery mq)
  {
    Double rc = mq.getRowCount(this);
    Double rs = mq.getSelectivity(this, null);
    if (rc != null && rs != null) {
      DruidMetadataProvider.LOG.debug("--> %s = %.2f + %.2f", partialQuery, rc, rs);
      return withContextOverride(Estimations.context(rc.longValue(), rs.floatValue()));
    }
    return this;
  }

  public static boolean appendEstimates(DruidQueryRel query)
  {
    return query.contextOverride.isEmpty() && !query.partialQuery.hasHaving();    // todo
  }

  @Override
  public void explain(RelWriter pw)
  {
    // to conform test explain results
    partialQuery.explainTerms(pw.item("table", Utils.qualifiedTableName(table)))
                .done(this);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return partialQuery.explainTerms(pw.item("table", Utils.qualifiedTableName(table)))
                       .itemIf("context", contextOverride.keySet(), !contextOverride.isEmpty());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq, Set<RelNode> visited)
  {
    double row = druidTable.getStatistic().getRowCount();
    RelOptCost cost = partialQuery.cost(druidTable, planner.getCostFactory());
//    if (visited.isEmpty()) {
//      System.out.printf("--> %s (%.2f) => %.2f (%.2f) : (%s)%n", druidTable.getDataSource(), row, cost.getCpu(), cost.getRows(), partialQuery);
//    }
    return cost;
  }
}
