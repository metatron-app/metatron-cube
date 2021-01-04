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

import javax.annotation.Nullable;
import java.util.List;

/**
 * DruidRel that uses a "query" dataSource.
 */
public class DruidOuterQueryRel extends DruidRel<DruidOuterQueryRel>
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
        sourceRel.getTraitSet(),
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
  public DruidOuterQueryRel withPartialQuery(final PartialDruidQuery newQueryBuilder)
  {
    return new DruidOuterQueryRel(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        sourceRel,
        newQueryBuilder,
        getQueryMaker()
    );
  }

  @Override
  public int getQueryCount()
  {
    final DruidRel druidRel = Utils.getDruidRel(sourceRel);
    return 1 + (druidRel == null ? 0 : druidRel.getQueryCount());
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
    return Preconditions.checkNotNull(Utils.getDruidRel(sourceRel)).getDataSourceNames();
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
    DruidQuery druidQuery = toDruidQueryForExplaining();
    return super.explainTerms(pw)
                .input("innerQuery", sourceRel)
                .item("query", toExplainString(druidQuery))
                .item("signature", druidQuery.getOutputRowSignature());
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeCost(mq.getRowCount(sourceRel), 0, 0).multiplyBy(10);
  }
}
