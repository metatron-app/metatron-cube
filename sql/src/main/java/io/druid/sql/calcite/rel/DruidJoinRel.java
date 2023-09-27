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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.collections.IntList;
import io.druid.query.CombinedDataSource;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.JoinElement;
import io.druid.query.JoinQuery;
import io.druid.query.JoinType;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.util.BitSet;
import java.util.List;
import java.util.Set;

public class DruidJoinRel extends DruidRel implements DruidRel.LeafRel
{
  public static DruidJoinRel create(Join join, JoinInfo joinInfo, DruidRel left, DruidRel right)
  {
    return new DruidJoinRel(
        join.getCluster(),
        join.getTraitSet(),
        join.getJoinType(),
        left,
        right,
        joinInfo.leftKeys,
        joinInfo.rightKeys,
        null,
        left.queryMaker
    );
  }

  private final JoinRelType joinType;
  private final ImmutableIntList leftExpressions;
  private final ImmutableIntList rightExpressions;
  private final ImmutableIntList outputColumns;

  private RelNode left;
  private RelNode right;

  private DruidJoinRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final JoinRelType joinType,
      final RelNode left,
      final RelNode right,
      final ImmutableIntList leftExpressions,
      final ImmutableIntList rightExpressions,
      final ImmutableIntList outputColumns,
      final QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.left = left;
    this.right = right;
    this.joinType = joinType;
    this.leftExpressions = leftExpressions;
    this.rightExpressions = rightExpressions;
    this.outputColumns = outputColumns;
  }

  public DruidJoinRel withOutputColumns(ImmutableIntList outputColumns)
  {
    return new DruidJoinRel(
        getCluster(),
        getTraitSet(),
        joinType,
        left,
        right,
        leftExpressions,
        rightExpressions,
        outputColumns,
        queryMaker
    );
  }

  public ImmutableIntList getOutputColumns()
  {
    return outputColumns;
  }

  public ImmutableIntList getLeftExpressions()
  {
    return leftExpressions;
  }

  public ImmutableIntList getRightExpressions()
  {
    return rightExpressions;
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    return withFinalize.get();
  }

  @Override
  public DruidQuery makeDruidQuery(boolean finalizeAggregations)
  {
    final RelDataType rowType = getRowType();
    final DruidRel leftRel = Utils.getDruidRel(left);
    final DruidRel rightRel = Utils.getDruidRel(right);
    if (leftRel == null || rightRel == null) {
      return null;
    }
    final DruidQuery leftQuery = leftRel.toDruidQuery(true);
    final DruidQuery rightQuery = rightRel.toDruidQuery(true);
    if (leftQuery == null || rightQuery == null) {
      return null;
    }
    final RowSignature leftSchema = leftQuery.getOutputRowSignature();
    final RowSignature rightSchema = rightQuery.getOutputRowSignature();

    final List<String> leftOrder = leftSchema.getColumnNames();
    final List<String> rightOrder = rightSchema.getColumnNames();

    final List<String> outputAlias = Utils.uniqueNames(leftOrder, rightOrder);
    final List<String> outputProjection;
    final RowSignature finalSignature;
    if (outputColumns != null) {
      List<String> extracted = Lists.newArrayList();
      for (int i = 0; i < outputColumns.size(); i++) {
        extracted.add(Preconditions.checkNotNull(outputAlias.get(outputColumns.get(i))));
      }
      outputProjection = extracted;
      finalSignature = RowSignature.from(outputProjection, rowType).unwrapDimensions();
    } else {
      outputProjection = null;
      finalSignature = RowSignature.from(outputAlias, rowType).unwrapDimensions();
    }

    final List<String> leftKeys = Lists.newArrayList();
    for (int leftKey : leftExpressions) {
      leftKeys.add(leftOrder.get(leftKey));
    }
    final List<String> rightKeys = Lists.newArrayList();
    for (int rightKey : rightExpressions) {
      rightKeys.add(rightOrder.get(rightKey));
    }

    final Query leftDruid = leftQuery.getQuery();
    final Query rightDruid = rightQuery.getQuery();
    final String leftAlias = toAlias(leftDruid);

    String rightAlias = toAlias(rightDruid);
    while (leftAlias.equals(rightAlias)) {
      rightAlias += "$";
    }

    final JoinQuery query = new Druids.JoinQueryBuilder()
        .dataSource(leftAlias, QueryDataSource.of(leftDruid, leftSchema))
        .dataSource(rightAlias, QueryDataSource.of(rightDruid, rightSchema))
        .element(new JoinElement(JoinType.fromString(joinType.name()), leftAlias, leftKeys, rightAlias, rightKeys))
        .outputAlias(outputAlias)
        .outputColumns(outputProjection)
        .context(getPlannerContext().copyQueryContext())
        .withSchema(finalSignature)
        .build();

    return new DruidQuery()
    {
      @Override
      public RelDataType getOutputRowType()
      {
        return rowType;
      }

      @Override
      public RowSignature getOutputRowSignature()
      {
        return finalSignature;
      }

      @Override
      public Query getQuery()
      {
        return query;
      }
    };
  }

  private String toAlias(Query query)
  {
    return StringUtils.join(query.getDataSource().getNames(), '+');
  }

  @Override
  public DruidJoinRel asDruidConvention()
  {
    return new DruidJoinRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        joinType,
        RelOptRule.convert(left, DruidConvention.instance()),
        RelOptRule.convert(right, DruidConvention.instance()),
        leftExpressions,
        rightExpressions,
        outputColumns,
        queryMaker
    );
  }

  @Override
  public DataSource getDataSource()
  {
    return CombinedDataSource.of(Utils.getDruidRel(left).getDataSource(), Utils.getDruidRel(right).getDataSource());
  }

  @Override
  protected RelDataType deriveRowType()
  {
    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    RelDataType rowType = SqlValidatorUtil.deriveJoinRowType(
        left.getRowType(),
        right.getRowType(),
        joinType,
        typeFactory,
        null,
        ImmutableList.of()
    );
    if (outputColumns != null) {
      List<String> fieldNames = Lists.newArrayList();
      List<RelDataType> fieldTypes = Lists.newArrayList();
      List<RelDataTypeField> fields = rowType.getFieldList();
      for (int i = 0; i < outputColumns.size(); i++) {
        RelDataTypeField field = fields.get(outputColumns.get(i));
        fieldNames.add(field.getName());
        fieldTypes.add(field.getType());
      }
      rowType = typeFactory.createStructType(fieldTypes, fieldNames);
    }
    return rowType;
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
    return new DruidJoinRel(
        getCluster(),
        traitSet,
        joinType,
        inputs.get(0),
        inputs.get(1),
        leftExpressions,
        rightExpressions,
        outputColumns,
        queryMaker
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return super.explainTerms(pw)
                .input("left", left)
                .input("right", right)
                .item("joinType", joinType)
                .itemIf("leftKeys", StringUtils.join(leftExpressions, ", "), !leftExpressions.isEmpty())
                .itemIf("rightKeys", StringUtils.join(rightExpressions, ", "), !rightExpressions.isEmpty())
                .itemIf("outputColumns", StringUtils.join(outputColumns, ", "), outputColumns != null);
  }

  public DruidJoinRel rewrite(DruidJoinRel input, boolean toLeft)
  {
    BitSet b0 = new BitSet();
    int leftLength = left.getRowType().getFieldList().size();
    int rightLength = right.getRowType().getFieldList().size();
    if (toLeft) {
      leftExpressions.forEachInt(b0::set);
      outputColumns.forEachInt(x -> {if (x < leftLength) b0.set(x);});
    } else {
      rightExpressions.forEachInt(b0::set);
      outputColumns.forEachInt(x -> {if (x >= leftLength) b0.set(x - leftLength);});
    }
    if (b0.cardinality() == (toLeft ? leftLength : rightLength)) {
      return this;
    }

    ImmutableIntList inputs = ImmutableIntList.of(b0.stream().toArray());

    IntList expressions = IntList.of();
    if (toLeft) {
      getLeftExpressions().forEachInt(x -> expressions.add(inputs.indexOf(x)));
    } else {
      getRightExpressions().forEachInt(x -> expressions.add(inputs.indexOf(x)));
    }

    IntList outputs = IntList.sizeOf(outputColumns.size());
    if (toLeft) {
      int delta = leftLength - inputs.size();
      outputColumns.forEachInt(x -> outputs.add(x < leftLength ? inputs.indexOf(x) : x - delta));
    } else {
      outputColumns.forEachInt(x -> outputs.add(x < leftLength ? x : inputs.indexOf(x - leftLength) + leftLength));
    }

    return new DruidJoinRel(
        getCluster(),
        getTraitSet(),
        joinType,
        toLeft ? input.withOutputColumns(inputs) : left,
        toLeft ? right : input.withOutputColumns(inputs),
        toLeft ? ImmutableIntList.of(expressions.array()) : leftExpressions,
        toLeft ? rightExpressions : ImmutableIntList.of(expressions.array()),
        ImmutableIntList.of(outputs.array()),
        queryMaker
    );
  }

  private static final double BLOOM_FILTER_REDUCTION = 0.7;
  private static final double HASH_JOIN_REDUCTION = 0.2;

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq, Set<RelNode> visited)
  {
    if (!visited.add(this)) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    final Pair<DruidRel, RelOptCost> lm = Utils.getMinimumCost(left, planner, mq, Sets.newHashSet(visited));
    if (lm.right.isInfinite()) {
      return lm.right;
    }
    final Pair<DruidRel, RelOptCost> rm = Utils.getMinimumCost(right, planner, mq, visited);
    if (rm.right.isInfinite()) {
      return rm.right;
    }

    final double lrow = lm.right.getRows();
    final double rrow = rm.right.getRows();

    double lc = lrow + 2;
    double rc = rrow + 2;

    boolean crossJoin = leftExpressions.isEmpty();

    double estimate;
    if (crossJoin) {
      estimate = lc * rc;
    } else if (joinType == JoinRelType.LEFT) {
      estimate = lc;
    } else if (joinType == JoinRelType.RIGHT) {
      estimate = rc;
    } else if (joinType == JoinRelType.FULL) {
      estimate = lc + rc;
    } else {
      // prefer larger difference
      estimate = Math.min(Math.max(lc, rc), Math.min(lc, rc) * 4) * Math.pow(0.6, leftExpressions.size());
    }

    if (!crossJoin) {
      if (joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT) {
        if (lm.left.hasFilter() && rm.right instanceof DruidQueryRel) {
          rc *= BLOOM_FILTER_REDUCTION;
        }
        if (lc > rc * 10) {
          lc *= HASH_JOIN_REDUCTION;
        }
      }
      if (joinType == JoinRelType.INNER || joinType == JoinRelType.RIGHT) {
        if (rm.left.hasFilter() && lm.right instanceof DruidQueryRel) {
          lc *= BLOOM_FILTER_REDUCTION;
        }
        if (rc > lc * 10) {
          rc *= HASH_JOIN_REDUCTION;
        }
      }
    }
    double cost = (crossJoin ? lc * rc : lc + rc) * PartialDruidQuery.JOIN_MULTIPLIER;
    if (lc > rc) {
      cost *= 0.999; // for deterministic plan
    }
//    if (visited.size() == 1) {
//      String n = crossJoin ? "CROSS" : joinType.name();
//      System.out.printf(">>>:%s %s + %s => %.2f (%.2f + %.2f = %.2f)%n", n, lm.left.getDataSource(), rm.left.getDataSource(), cost, lrow, rrow, estimate);
//    }
    return planner.getCostFactory().makeCost(estimate, cost, 0);
  }
}
