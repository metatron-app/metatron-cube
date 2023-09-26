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
import io.druid.java.util.common.logger.Logger;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.rel.DruidQueryRel;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.PartialDruidQuery;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.rule.DruidCost;
import io.druid.sql.calcite.table.DruidTable;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;

import java.util.List;

public class DruidMetadataProvider
{
  public static final ThreadLocal<QueryMaker> CONTEXT = new ThreadLocal<>();

  public static final Logger LOG = new Logger(DruidMetadataProvider.class);

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

    public Double getSelectivity(DruidRel druidRel, RelMetadataQuery mq, RexNode predicate)
    {
      PartialDruidQuery partialQuery = druidRel.getPartialDruidQuery();
      if (druidRel instanceof DruidQueryRel && partialQuery.getAggregateFilter() != null) {
        DruidTable table = Utils.unwrapTable(partialQuery.getScan(), DruidTable.class);
        Double selectivity = table == null ? null : table.estimateHavingSelectivity(druidRel, mq, CONTEXT.get());
        if (selectivity != null) {
          return selectivity;
        }
      }
      return mq.getSelectivity(druidRel.getLeafRel(), null);
    }

    public Double getSelectivity(RelSubset rel, RelMetadataQuery mq, RexNode predicate)
    {
      return mq.getSelectivity(rel.stripped(), predicate);
    }

    public Double getSelectivity(TableScan scan, RelMetadataQuery mq, RexNode predicate)
    {
      if (predicate == null || predicate.isAlwaysTrue()) {
        return 1.0D;
      }
      DruidTable table = scan.getTable().unwrap(DruidTable.class);
      if (table == null) {
        return Utils.selectivity(scan, predicate);
      }
      long p = System.currentTimeMillis();
      Double selectivity = table.estimateSelectivity(scan, predicate, CONTEXT.get());
      if (selectivity != null) {
        long elapsed = System.currentTimeMillis() - p;
        LOG.debug("--- selectivity [%s] : %s = %.3f (%d msec)", table.getName(), predicate, selectivity, elapsed);
        return selectivity;
      }
      return Utils.selectivity(scan, predicate);
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

    public Double getDistinctRowCount(DruidRel druidRel, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate)
    {
      return mq.getDistinctRowCount(druidRel.getLeafRel(), groupKey, predicate);
    }

    public Double getDistinctRowCount(RelSubset rel, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate)
    {
      return mq.getDistinctRowCount(rel.stripped(), groupKey, predicate);
    }

    public Double getDistinctRowCount(TableScan scan, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate)
    {
      if (groupKey.isEmpty()) {
        return 1D;
      }
      DruidTable table = scan.getTable().unwrap(DruidTable.class);
      if (table == null) {
        return null;
      }
      long p = System.currentTimeMillis();
      List<String> keys = table.getRowSignature().columnNames(groupKey);
      Double estimation = table.estimateCardinality(scan, mq, keys, predicate, CONTEXT.get());
      long elapsed = System.currentTimeMillis() - p;
      if (estimation != null && Double.isFinite(estimation)) {
        LOG.debug("--- cardinality [%s]%s%s = %.1f, %,d msec", table.getName(), keys, lazy(predicate), estimation, elapsed);
        return estimation;
      }
      return null;
    }
  }

  private static Object lazy(RexNode predicate)
  {
    return Logs.lazy(" : %s", predicate);
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

    public Double getRowCount(DruidRel druidRel, RelMetadataQuery mq)
    {
      PartialDruidQuery partialQuery = druidRel.getPartialDruidQuery();
      if (druidRel instanceof DruidQueryRel && partialQuery.getAggregateFilter() != null) {
        Double rc = mq.getRowCount(partialQuery.getAggregate());
        if (rc != null) {
          return rc * mq.getSelectivity(druidRel, null);
        }
      }
      return mq.getRowCount(druidRel.getLeafRel());
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
