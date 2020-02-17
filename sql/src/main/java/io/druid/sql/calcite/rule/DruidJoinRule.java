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

package io.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.KeyedData.StringKeyed;
import io.druid.java.util.common.logger.Logger;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.rel.DruidJoinRel;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.DruidSpatialJoinRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

public class DruidJoinRule extends RelOptRule
{
  private static final DruidJoinRule INSTANCE = new DruidJoinRule();

  public static DruidJoinRule instance()
  {
    return INSTANCE;
  }

  private DruidJoinRule()
  {
    super(
        operand(
            Join.class, some(operand(DruidRel.class, any()), operand(DruidRel.class, any()))
        )
    );
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Join join = call.rel(0);
    final DruidRel left = call.rel(1);
    final DruidRel right = call.rel(2);

    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RexNode condition = extractCommonInOr(join.getCondition(), rexBuilder);
    final JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), condition);
    if (joinInfo.isEqui()) {
      call.transformTo(DruidJoinRel.create(join, joinInfo, left, right));
    } else if (!joinInfo.leftKeys.isEmpty() && join.getJoinType() == JoinRelType.INNER) {
      RexNode remaining = joinInfo.getRemaining(rexBuilder);
      DruidRel joinRel = DruidJoinRel.create(join, joinInfo, left, right);
      call.transformTo(new LogicalFilter(
          join.getCluster(),
          join.getTraitSet(),
          joinRel,
          remaining,
          ImmutableSet.of()
      ));
    } else if (joinInfo.leftKeys.isEmpty() && join.getJoinType() == JoinRelType.INNER) {
      RelNode transformed = trySpatialJoin(left, right, condition);
      if (transformed != null) {
        call.transformTo(transformed);
      }
    }
  }

  private static final Logger LOG = new Logger(DruidJoinRule.class);

  private static final Set<String> SUPPORTED = ImmutableSet.of(
      "ST_CONTAINS", "ST_WITHIN", "GEOM_CONTAINS", "GEOM_WITHIN"
  );

  private RelNode trySpatialJoin(DruidRel left, DruidRel right, RexNode condition)
  {
    final String opName = Utils.opName(condition);
    if (SUPPORTED.contains(opName)) {
      int[] refs = Utils.getInputRefs(((RexCall) condition).getOperands());
      if (refs == null || refs.length != 2) {
        return null;
      }

      final int leftCount = left.getRowType().getFieldList().size();
      if (opName.endsWith("CONTAINS")) {
        // B JOIN N ST_CONTAINS(b, n) --> query N boundary B, flip
        // N JOIN B ST_CONTAINS(b, n) --> query N boundary B
        if (refs[0] < leftCount && refs[1] >= leftCount) {
          return DruidSpatialJoinRel.create(right, left, refs[1] - leftCount, refs[0], true);
        } else if (refs[0] >= leftCount && refs[1] < leftCount) {
          return DruidSpatialJoinRel.create(left, right, refs[1], refs[0] - leftCount, false);
        }
      } else {
        // N JOIN B ST_WITHIN(n, b) --> query N boundary B
        // B JOIN N ST_WITHIN(n, b) --> query N boundary B, flip
        if (refs[0] < leftCount && refs[1] >= leftCount) {
          return DruidSpatialJoinRel.create(left, right, refs[0], refs[1] - leftCount, false);
        } else if (refs[0] >= leftCount && refs[1] < leftCount) {
          return DruidSpatialJoinRel.create(right, left, refs[0] - leftCount, refs[1], true);
        }
      }
    }
    return null;
  }

  // I'm not sure of this
  // for tpch-19 : OR ( AND(A, B), AND(A, C), AND(A, D)) --> AND (A, OR (B, C, D))
  private RexNode extractCommonInOr(RexNode condition, RexBuilder builder)
  {
    if (!Utils.isOr(condition)) {
      return condition;
    }
    List<RexNode> operands = ((RexCall) condition).getOperands();
    if (operands.size() == 1) {
      return operands.get(0);
    }
    @SuppressWarnings("unchecked")
    List<StringKeyed<RexNode>>[] nodeList = new List[operands.size()];

    int index = 0;
    nodeList[index] = getAndedNodes(operands.get(index));
    List<StringKeyed<RexNode>> common = Lists.newArrayList(nodeList[0]);
    List<String> commonString = Lists.newArrayList();
    for (index++; index < nodeList.length; index++) {
      nodeList[index] = getAndedNodes(operands.get(index));
      common.retainAll(nodeList[index]);
    }
    if (common.isEmpty()) {
      return condition;
    }
    List<RexNode> orOperands = Lists.newArrayList();
    for (int i = 0; i < nodeList.length; i++) {
      nodeList[i].removeAll(common);
      if (nodeList[i].isEmpty()) {
        // OR ( AND(A, B), AND(A, C), A) --> A
        // OR ( AND(A, B, C), AND(A, B, D), AND(A, B)) --> AND(A, B)
        return Utils.and(builder, StringKeyed.values(common));
      }
      if (nodeList[i].size() == 1) {
        orOperands.add(nodeList[i].get(0).value());
      } else {
        // OR ( AND(A, B, C), AND(A, D), AND(A, E)) --> AND(A, OR( AND(B, C), D, E))
        orOperands.add(Utils.and(builder, StringKeyed.values(nodeList[i])));
      }
    }
    return Utils.and(builder, GuavaUtils.concat(StringKeyed.values(common), Utils.or(builder, orOperands)));
  }

  @SuppressWarnings("unchecked")
  private List<StringKeyed<RexNode>> getAndedNodes(RexNode op)
  {
    if (Utils.isAnd(op)) {
      return StringKeyed.of(((RexCall) op).getOperands());
    } else {
      return Lists.newArrayList(StringKeyed.of(op));
    }
  }
}
