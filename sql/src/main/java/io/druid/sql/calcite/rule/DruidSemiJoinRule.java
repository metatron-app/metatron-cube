/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package io.druid.sql.calcite.rule;

import com.metamx.common.logger.Logger;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.groupby.GroupByQuery;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.rel.DruidQuery;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.DruidSemiJoinRel;
import io.druid.sql.calcite.rel.PartialDruidQuery;
import io.druid.sql.calcite.rel.QueryMaker;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.UUID;
import java.util.function.Predicate;

/**
 * Planner rule adapted from Calcite 1.11.0's SemiJoinRule.
 *
 * This rule identifies a JOIN where the right-hand side is being used like a filter. Requirements are:
 *
 * 1) Right-hand side is grouping on the join key
 * 2) No fields from the right-hand side are selected
 * 3) Join is INNER (right-hand side acting as filter) or LEFT (right-hand side can be ignored)
 *
 * This is used instead of Calcite's built in rule because that rule's un-doing of aggregation is unproductive (we'd
 * just want to add it back again). Also, this rule operates on DruidRels.
 */
public class DruidSemiJoinRule extends RelOptRule
{
  private static final Logger LOG = new Logger(DruidSemiJoinRule.class);

  private static final Predicate<Join> IS_LEFT_OR_INNER =
      join -> {
        final JoinRelType joinType = join.getJoinType();
        return joinType == JoinRelType.LEFT || joinType == JoinRelType.INNER;
      };

  private static final Predicate<DruidRel> IS_GROUP_BY = druidRel ->
      druidRel.getPartialDruidQuery() != null && druidRel.getPartialDruidQuery().getAggregate() != null;

  private static final DruidSemiJoinRule INSTANCE = new DruidSemiJoinRule();

  private DruidSemiJoinRule()
  {
    super(
        operand(
            Project.class,
            operandJ(
                Join.class,
                null,
                IS_LEFT_OR_INNER,
                some(
                    operandJ(
                        DruidRel.class,
                        null,
                        DruidRules.CAN_BUILD_ON.and(IS_GROUP_BY.negate()),
                        any()
                    ),
                    operandJ(DruidRel.class, null, IS_GROUP_BY, any())
                )
            )
        )
    );
  }

  public static DruidSemiJoinRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Project project = call.rel(0);
    final Join join = call.rel(1);
    final DruidRel left = call.rel(2);
    final DruidRel right = call.rel(3);

    final ImmutableBitSet bits =
        RelOptUtil.InputFinder.bits(project.getProjects(), null);
    final ImmutableBitSet rightBits =
        ImmutableBitSet.range(
            left.getRowType().getFieldCount(),
            join.getRowType().getFieldCount()
        );

    if (bits.intersects(rightBits)) {
      return;
    }

    final JoinInfo joinInfo = join.analyzeCondition();

    // Rule requires that aggregate key to be the same as the join key.
    // By the way, neither a super-set nor a sub-set would work.

    if (!joinInfo.isEqui() ||
        joinInfo.rightSet().cardinality() != right.getPartialDruidQuery().getAggregate().getGroupCount()) {
      return;
    }

    final PartialDruidQuery rightQuery = right.getPartialDruidQuery();
    final Project rightProject = rightQuery.getSortProject() != null ?
                                 rightQuery.getSortProject() :
                                 rightQuery.getAggregateProject();
    int i = 0;
    for (int joinRef : joinInfo.rightSet()) {
      final int aggregateRef;

      if (rightProject == null) {
        aggregateRef = joinRef;
      } else {
        final RexNode projectExp = rightProject.getChildExps().get(joinRef);
        if (projectExp.isA(SqlKind.INPUT_REF)) {
          aggregateRef = ((RexInputRef) projectExp).getIndex();
        } else {
          // Project expression is not part of the grouping key.
          return;
        }
      }

      if (aggregateRef != i++) {
        return;
      }
    }

    final RelBuilder relBuilder = call.builder();

    if (join.getJoinType() == JoinRelType.LEFT) {
      // Join can be eliminated since the right-hand side cannot have any effect (nothing is being selected,
      // and LEFT means even if there is no match, a left-hand row will still be included).
      relBuilder.push(left);
    } else {
      final DruidQuery druidQuery = right.toDruidQuery(true);
      if (druidQuery == null) {
        return;
      }
      int maxSegmiJoinRows = left.getPlannerContext().getPlannerConfig().getMaxSemiJoinRowsInMemory();
      final Query query = right.toDruidQuery(true).getQuery();
      if (query instanceof GroupByQuery) {
        GroupByQuery groupBy = (GroupByQuery) query.withId(UUID.randomUUID().toString());
        QueryMaker queryMaker = right.getQueryMaker();
        long cardinality = Queries.estimateCardinality(
            groupBy.removePostActions(),
            queryMaker.getSegmentWalker(),
            queryMaker.getQueryConfig()
        );
        if (cardinality < 0 || cardinality > maxSegmiJoinRows) {
          LOG.info("Estimated cardinality [%d] is exceeding maxSegmiJoinRows [%d]", cardinality, maxSegmiJoinRows);
          return;
        }
      }
      final DruidSemiJoinRel druidSemiJoin = DruidSemiJoinRel.create(
          left,
          right,
          joinInfo.leftKeys,
          joinInfo.rightKeys,
          maxSegmiJoinRows
      );

      // Check maxQueryCount.
      final PlannerConfig plannerConfig = left.getPlannerContext().getPlannerConfig();
      if (plannerConfig.getMaxQueryCount() > 0 && druidSemiJoin.getQueryCount() > plannerConfig.getMaxQueryCount()) {
        return;
      }

      relBuilder.push(druidSemiJoin);
    }

    call.transformTo(
        relBuilder
            .project(project.getProjects(), project.getRowType().getFieldNames())
            .build()
    );
  }
}
