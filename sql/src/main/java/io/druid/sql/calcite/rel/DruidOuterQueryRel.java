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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.common.utils.StringUtils;
import io.druid.query.QueryDataSource;
import io.druid.query.TableDataSource;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * DruidRel that uses a "query" dataSource.
 */
public class DruidOuterQueryRel extends DruidRel
{
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__subquery__");

  private final PartialDruidQuery partialQuery;
  private RelNode sourceRel;

  private DruidOuterQueryRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode sourceRel,
      PartialDruidQuery partialQuery,
      QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.sourceRel = Preconditions.checkNotNull(sourceRel);
    this.partialQuery = Preconditions.checkNotNull(partialQuery);
  }

  public static DruidOuterQueryRel create(
      final DruidRel sourceRel,
      final PartialDruidQuery partialQuery
  )
  {
    return new DruidOuterQueryRel(
        sourceRel.getCluster(),
        sourceRel.getTraitSet().plus(partialQuery.getCollation()),
        sourceRel,
        partialQuery,
        sourceRel.getQueryMaker()
    );
  }

  public RelNode getSourceRel()
  {
    return sourceRel;
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public boolean hasFilter()
  {
    return partialQuery.getScanFilter() != null;
  }

  @Override
  public DruidOuterQueryRel withPartialQuery(final PartialDruidQuery newQueryBuilder)
  {
    return new DruidOuterQueryRel(
        getCluster(),
        getTraitSet().plus(newQueryBuilder.getCollation()),
        sourceRel,
        newQueryBuilder,
        getQueryMaker()
    );
  }

  @Nullable
  @Override
  public DruidQuery makeDruidQuery(final boolean finalizeAggregations)
  {
    // Must finalize aggregations on subqueries.

    final DruidRel druidRel = Utils.getDruidRel(sourceRel);
    if (druidRel == null) {
      return null;
    }
    final DruidQuery subQuery = druidRel.toDruidQuery(true);
    if (subQuery == null) {
      return null;
    }

    final RowSignature sourceRowSignature = subQuery.getOutputRowSignature();
    return partialQuery.build(
        QueryDataSource.of(subQuery.getQuery()),
        sourceRowSignature,
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations
    );
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    // cannot use toDruidQuery() cause sourceRel is in Druid convension..
    return partialQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignature.from(sourceRel.getRowType()),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false
    );
  }

  @Override
  public DruidOuterQueryRel asDruidConvention()
  {
    return new DruidOuterQueryRel(
        getCluster(),
        getTraitSet().plus(DruidConvention.instance()),
        RelOptRule.convert(sourceRel, DruidConvention.instance()),
        partialQuery,
        getQueryMaker()
    );
  }

  @Override
  public List<String> getDataSourceNames()
  {
    return Utils.getDruidRel(sourceRel).getDataSourceNames();
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(sourceRel);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    if (ordinalInParent != 0) {
      throw new IndexOutOfBoundsException(StringUtils.format("Invalid ordinalInParent[%s]", ordinalInParent));
    }
    this.sourceRel = p;
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidOuterQueryRel(
        getCluster(),
        traitSet,
        Iterables.getOnlyElement(inputs),
        getPartialDruidQuery(),
        getQueryMaker()
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return partialQuery.explainTerms(pw.input("input", sourceRel));
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq, Set<RelNode> visited)
  {
    if (!visited.add(this)) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    final Pair<DruidRel, RelOptCost> m = Utils.getMinimumCost(sourceRel, planner, mq, visited);
    if (m.right.isInfinite()) {
      return m.right;
    }
    final double count = m.right.getRows();
    final double estimate = partialQuery.cost(count);
//    if (Iterables.getFirst(visited, null) == this) {
//      System.out.println(String.format("> %s : %f => %f", m.left.getDataSourceNames(), count, estimate));
//    }
    return planner.getCostFactory().makeCost(estimate, 0, 0);
  }
}
