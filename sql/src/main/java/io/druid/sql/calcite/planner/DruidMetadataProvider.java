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

package io.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.common.utils.Logs;
import io.druid.data.input.Row;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.rel.PartialDruidQuery;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.rule.DruidCost;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Collation;
import org.apache.calcite.rel.metadata.BuiltInMetadata.DistinctRowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.NonCumulativeCost;
import org.apache.calcite.rel.metadata.BuiltInMetadata.RowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Selectivity;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;

import javax.annotation.Nullable;
import java.util.List;

public class DruidMetadataProvider
{
  public static final ThreadLocal<QueryMaker> CONTEXT = new ThreadLocal<>();

  private static final Logger LOG = new Logger(DruidMetadataProvider.class);

  public static final RelMetadataProvider INSTANCE = ChainedRelMetadataProvider.of(
      ImmutableList.of(
          DruidCostModel.SOURCE,
          DruidRelMdSelectivity.SOURCE,
          DruidRelMdRowCount.SOURCE,
          DruidRelMdDistinctRowCount.SOURCE,
          DruidRelMdCollation.SOURCE,
          DefaultRelMetadataProvider.INSTANCE
      )
  );

  public static class DruidCostModel implements MetadataHandler<NonCumulativeCost>
  {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            new DruidCostModel(), NonCumulativeCost.Handler.class);

    @Override
    public MetadataDef<NonCumulativeCost> getDef()
    {
      return NonCumulativeCost.DEF;
    }

    public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq)
    {
      return rel.computeSelfCost(rel.getCluster().getPlanner(), mq);
    }

    public RelOptCost getNonCumulativeCost(Filter rel, RelMetadataQuery mq)
    {
      double rc = mq.getRowCount(rel.getInput());
      double cost = rc * Utils.rexEvalCost(rel.getCondition());
      return DruidCost.FACTORY.makeCost(0, cost, 0);
    }

    public RelOptCost getNonCumulativeCost(Project rel, RelMetadataQuery mq)
    {
      double rc = mq.getRowCount(rel.getInput());
      double cost = rc * Utils.rexEvalCost(rel.getProjects());
      return DruidCost.FACTORY.makeCost(0, cost, 0);
    }

    public RelOptCost getNonCumulativeCost(Sort rel, RelMetadataQuery mq)
    {
      double rc = mq.getRowCount(rel.getInput());
      double cost = 0;
      if (!rel.getSortExps().isEmpty()) {
        cost += rc * PartialDruidQuery.SORT_MULTIPLIER;
      }
      return DruidCost.FACTORY.makeCost(0, cost, 0);
    }

    public RelOptCost getNonCumulativeCost(Join rel, RelMetadataQuery mq)
    {
      RelNode left = rel.getLeft();
      RelNode right = rel.getRight();
      double rc1 = Math.max(mq.getRowCount(left), 1);
      double rc2 = Math.max(mq.getRowCount(right), 1);
//      ImmutableBitSet leftKeys = rel.analyzeCondition().leftSet();
//      ImmutableBitSet rightKeys = rel.analyzeCondition().rightSet();
//      double drc1 = mq.getDistinctRowCount(left, leftKeys, null);
//      double drc2 = mq.getDistinctRowCount(right, rightKeys, null);
//      LOG.debug("%s%s : %f/%f + %s%s : %f/%f",
//                Utils.alias(left), Utils.columnNames(left, leftKeys), drc1, rc1,
//                Utils.alias(right), Utils.columnNames(right, rightKeys), drc2, rc2);
      return DruidCost.FACTORY.makeCost(0, Utils.joinCost(rc1, rc2), 0);
    }

    public RelOptCost getNonCumulativeCost(Aggregate rel, RelMetadataQuery mq)
    {
      int groupings = rel.getGroupSets().size();
      int dimensionality = rel.getGroupSet().cardinality();

      double rc = mq.getRowCount(rel.getInput());
      double cost = rc * Utils.aggregationCost(dimensionality, rel.getAggCallList()) * groupings;
      return DruidCost.FACTORY.makeCost(0, cost, 0);
    }
  }

  public static class DruidRelMdSelectivity implements MetadataHandler<Selectivity>
  {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            new DruidRelMdSelectivity(), Selectivity.Handler.class);

    @Override
    public MetadataDef<Selectivity> getDef()
    {
      return Selectivity.DEF;
    }

    public Double getSelectivity(RelNode rel, RelMetadataQuery mq, RexNode predicate)
    {
      return Utils.selectivity(rel, predicate);
    }

//    public Double getSelectivity(Project rel, RelMetadataQuery mq, RexNode predicate)
//    {
//      List<RexNode> notPushable = Lists.newArrayList();
//      List<RexNode> pushable = Lists.newArrayList();
//      ImmutableBitSet bitmap = ImmutableBitSet.range(rel.getRowType().getFieldCount());
//      RelOptUtil.splitFilters(bitmap, predicate, pushable, notPushable);
//
//      RexBuilder builder = rel.getCluster().getRexBuilder();
//      RexNode childPred = RexUtil.composeConjunction(builder, pushable, true);
//
//      RexNode pushedPred = childPred == null ? null : RelOptUtil.pushPastProject(childPred, rel);
//      Double selectivity = mq.getSelectivity(rel.getInput(), pushedPred);
//      if (selectivity == null || notPushable.isEmpty()) {
//        return selectivity;
//      }
//      RexNode pred = RexUtil.composeConjunction(builder, notPushable, true);
//      return selectivity * Utils.selectivity(rel, pred);
//    }

    public Double getSelectivity(TableScan scan, RelMetadataQuery mq, RexNode predicate)
    {
      if (predicate == null || predicate.isAlwaysTrue()) {
        return Utils.selectivity(scan, predicate);
      }
      QueryMaker context = CONTEXT.get();
      if (context == null || !context.getPlannerContext().getPlannerConfig().isEstimateSelectivity()) {
        return Utils.selectivity(scan, predicate);
      }
      return context.selectivity(scan, predicate, k -> selectivity(scan, predicate, context));
    }

    // todo move this to DruidTable
    private static Double selectivity(TableScan rel, RexNode predicate, QueryMaker context)
    {
      long p = System.currentTimeMillis();
      Filtration filtration = toFiltration(rel, predicate, context, RowSignature.from(rel.getRowType()));
      if (filtration == null) {
        return null;
      }
      String table = Utils.tableName(rel.getTable());
      long[] estimation = Utils.estimateSelectivity(table, filtration, context);
      double selectivity = estimation[1] == 0 ? 0D : estimation[0] / (double) estimation[1];
      LOG.debug("--- selectivity(%s : %s) = %.3f (%d msec)", table, predicate, selectivity, System.currentTimeMillis() - p);
      return selectivity;
    }
  }

  public static class DruidRelMdDistinctRowCount implements MetadataHandler<DistinctRowCount>
  {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            new DruidRelMdDistinctRowCount(), DistinctRowCount.Handler.class);

    @Override
    public MetadataDef<DistinctRowCount> getDef()
    {
      return DistinctRowCount.DEF;
    }

    public Double getDistinctRowCount(TableScan scan, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate)
    {
      if (groupKey.cardinality() == 0) {
        return Double.valueOf(1);
      }
      DruidTable table = scan.getTable().unwrap(DruidTable.class);
      if (table == null) {
        return null;
      }
      List<String> keys = table.getRowSignature().columnNames(groupKey);
      if (keys.contains(Row.TIME_COLUMN_NAME)) {
        return null;    // todo ??
      }
      long cardinality = table.estimateCardinality(scan, mq, groupKey, predicate);
      if (cardinality >= 0) {
        LOG.debug("--- cardinality(%s%s%s) = %d", table.getName(), keys, Logs.lazy(" : %s", predicate), cardinality);
        return Double.valueOf(cardinality);
      }
      QueryMaker context = CONTEXT.get();
      if (context != null && context.getPlannerContext().getPlannerConfig().isEstimateCardinality()) {
        return context.cardinality(scan, groupKey, predicate, k -> cardinality(scan, groupKey, predicate, context));
      }
      return null;
    }

    // todo move this to DruidTable
    private static Double cardinality(TableScan rel, ImmutableBitSet groupKey, RexNode predicate, QueryMaker context)
    {
      long p = System.currentTimeMillis();
      RowSignature signature = RowSignature.from(rel.getRowType());
      Filtration filtration = toFiltration(rel, predicate, context, signature);
      if (filtration == null) {
        return null;
      }
      String table = Utils.tableName(rel.getTable());
      List<String> keys = signature.columnNames(groupKey);
      long estimation = Utils.estimateCardinality(table, filtration, keys, context);
      long elapsed = System.currentTimeMillis() - p;
      LOG.debug("--- cardinality(%s%s%s) = %d (%d msec)", table, keys, Logs.lazy(" : %s", predicate), estimation, elapsed);
      return (double) estimation;
    }
  }

  @Nullable
  private static Filtration toFiltration(RelNode rel, RexNode predicate, QueryMaker context, RowSignature signature)
  {
    if (predicate == null) {
      return Filtration.create(null);
    }
    RexBuilder builder = rel.getCluster().getRexBuilder();
    DimFilter filter = Expressions.toFilter(context.getPlannerContext(), signature, builder, predicate);
    return filter == null ? null : Filtration.create(filter).optimize(signature);
  }

  public static class DruidRelMdRowCount implements MetadataHandler<RowCount>
  {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            new DruidRelMdRowCount(), RowCount.Handler.class);

    @Override
    public MetadataDef<RowCount> getDef()
    {
      return RowCount.DEF;
    }

    public Double getRowCount(Join join, RelMetadataQuery mq)
    {
      JoinRelType type = join.getJoinType();
      if (!type.projectsRight()) {
        RexNode semiJoinSelectivity = RelMdUtil.makeSemiJoinSelectivityRexNode(mq, join);
        return NumberUtil.multiply(
            mq.getSelectivity(join.getLeft(), semiJoinSelectivity), mq.getRowCount(join.getLeft()));
      }
      double rc1 = Math.max(mq.getRowCount(join.getLeft()), 1);
      double rc2 = Math.max(mq.getRowCount(join.getRight()), 1);
      if (join.analyzeCondition().leftKeys.isEmpty()) {
        return rc1 * rc2;
      }
      if (type == JoinRelType.INNER) {
        double s1 = mq.getSelectivity(join.getLeft(), null);
        double s2 = mq.getSelectivity(join.getRight(), null);
        double delta = 1 - Math.abs(s1 - s2);
        if (s1 > s2) {
          rc1 *= delta;
        } else {
          rc2 *= delta;
        }
        if (rc1 > rc2) {
          rc1 /= 1 + Math.log10(rc1 / rc2);
        } else {
          rc2 /= 1 + Math.log10(rc2 / rc1);
        }
      }
      switch (type) {
        case INNER:
          return Math.max(rc1, rc2);
        case LEFT:
          return rc1;
        case RIGHT:
          return rc2;
        case FULL:
          return Math.max(rc1, rc2);
      }
      return RelMdUtil.getJoinRowCount(mq, join, join.getCondition());
    }

    public Double getRowCount(Aggregate rel, RelMetadataQuery mq)
    {
      int groupings = rel.getGroupSets().size();
      int cardinality = rel.getGroupSet().cardinality();

      double rc = mq.getRowCount(rel.getInput());
      return rc * Utils.aggregationRow(cardinality) * groupings;
    }

    public Double getRowCount(Sort rel, RelMetadataQuery mq)
    {
      double rc = mq.getRowCount(rel.getInput());
      if (rel.fetch != null) {
        rc = Math.min(rc, RexLiteral.intValue(rel.fetch));
      }
      return rc;
    }
  }

  public static class DruidRelMdCollation implements MetadataHandler<Collation>
  {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            new DruidRelMdCollation(), Collation.Handler.class);

    @Override
    public MetadataDef<Collation> getDef()
    {
      return Collation.DEF;
    }

    public ImmutableList<RelCollation> collations(Aggregate aggregate, RelMetadataQuery mq)
    {
      if (Utils.distributed(aggregate) && aggregate.groupSets.size() == 1) {
        List<RelFieldCollation> collations = Lists.newArrayList();
        aggregate.getGroupSet().forEachInt(x -> collations.add(new RelFieldCollation(x)));
        return ImmutableList.of(RelCollations.of(collations));
      }
      return null;
    }
  }
}
