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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.CombinedDataSource;
import io.druid.query.DataSource;
import io.druid.sql.calcite.Utils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

public class DruidCorrelateRel extends DruidRel
{
  public static RelNode of(Correlate correlate, DruidRel left, DruidRel right)
  {
    return new DruidCorrelateRel(
        correlate.getCluster(),
        correlate.getTraitSet(),
        correlate,
        left,
        right,
        PartialDruidQuery.create(correlate, left.getPlannerContext()),
        left.queryMaker
    );
  }

  private RelNode left;
  private RelNode right;
  private final Correlate source;
  private final PartialDruidQuery partialQuery;

  private DruidCorrelateRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      Correlate source,
      RelNode left,
      RelNode right,
      PartialDruidQuery partialQuery,
      QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.left = left;
    this.right = right;
    this.source = source;
    this.partialQuery = partialQuery;
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return new DruidCorrelateRel(
        getCluster(),
        getTraitSet().plus(newQueryBuilder.getCollation()),
        source,
        left,
        right,
        newQueryBuilder,
        queryMaker
    );
  }

  @Override
  public DruidCorrelateRel asDruidConvention()
  {
    return new DruidCorrelateRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        source,
        RelOptRule.convert(left, DruidConvention.instance()),
        RelOptRule.convert(right, DruidConvention.instance()),
        partialQuery,
        queryMaker
    );
  }

  @Override
  public DataSource getDataSource()
  {
    return CombinedDataSource.of(Utils.getDruidRel(left).getDataSource(), Utils.getDruidRel(right).getDataSource());
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(left, right);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    switch (ordinalInParent) {
      case 0:
        this.left = p;
        break;
      case 1:
        this.right = p;
        break;
      default:
        throw new IndexOutOfBoundsException("Input " + ordinalInParent);
    }
    recomputeDigest();
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidCorrelateRel(
        getCluster(),
        traitSet,
        source,
        inputs.get(0),
        inputs.get(1),
        partialQuery,
        queryMaker
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return partialQuery.explainTerms(
        super.explainTerms(pw)
             .input("left", left)
             .input("right", right)
             .item("correlation", source.getCorrelationId())
    );
  }


  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.leafRel().getRowType();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq, Set<RelNode> visited)
  {
    if (!visited.add(this)) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    final Pair<DruidRel, RelOptCost> m = Utils.getMinimumCost(left, planner, mq, visited);
    if (m.right.isInfinite()) {
      return m.right;
    }
    final double count = m.right.getRows();
    return partialQuery.cost(count * 2, planner.getCostFactory());
  }

  @Nullable
  @Override
  public DruidQuery makeDruidQuery(boolean finalizeAggregations)
  {
    DruidRel[] rels = new DruidRel[]{Utils.getDruidRel(left), Utils.getDruidRel(right)};
    if (rels[0] == null || rels[1] == null ||
        rels[0] instanceof DruidTableFunctionScanRel == rels[1] instanceof DruidTableFunctionScanRel) {
      return null;
    }
    PartialDruidQuery[] partials = new PartialDruidQuery[]{
        rels[0].getPartialDruidQuery(), rels[1].getPartialDruidQuery()
    };
    if (partials[0] == null || !partials[0].isFilterScanOnly() ||
        partials[1] == null || !partials[1].isFilterScanOnly()) {
      return null;
    }
    if (rels[0] instanceof DruidTableFunctionScanRel) {
      GuavaUtils.swap(rels, 0, 1);
      GuavaUtils.swap(partials, 0, 1);
    }
    PartialDruidQuery merged = partialQuery.withScan(partials[0].getScan());
    if (partials[0].getScanFilter() != null) {
      merged = merged.mergeScanFilter(partials[0].getScanFilter());
    }
    merged = merged.withTableFunction(partials[1]);

    DruidQuery query = rels[0].makeDruidQuery(finalizeAggregations);
    return merged.build(
        query.getQuery().getDataSource(),
        query.getOutputRowSignature(),
        getPlannerContext(),
        ImmutableMap.of(),
        getCluster().getRexBuilder(),
        finalizeAggregations
    );
  }
}
