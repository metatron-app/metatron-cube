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
import io.druid.sql.calcite.rel.DruidQueryRel;
import io.druid.sql.calcite.rel.DruidRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;

import java.util.List;
import java.util.Set;

import static io.druid.sql.calcite.rel.DruidSpatialJoinRel.create;

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
      RexNode remaining = RexUtil.composeConjunction(rexBuilder, joinInfo.nonEquiConditions);
      DruidRel joinRel = DruidJoinRel.create(join, joinInfo, left, right);
      call.transformTo(new LogicalFilter(
          join.getCluster(),
          join.getTraitSet(),
          joinRel,
          remaining,
          ImmutableSet.of()
      ));
    } else if (joinInfo.leftKeys.isEmpty() && join.getJoinType() == JoinRelType.INNER) {
      RelNode transformed = trySpatialJoin(left, right, condition, rexBuilder);
      if (transformed != null) {
        call.transformTo(transformed);
      }
    }
  }

  private static final Logger LOG = new Logger(DruidJoinRule.class);

  private static final Set<String> SUPPORTED = ImmutableSet.of(
      "ST_CONTAINS", "ST_WITHIN", "GEOM_CONTAINS", "GEOM_WITHIN"
  );

  private RelNode trySpatialJoin(DruidRel left, DruidRel right, RexNode condition, RexBuilder builder)
  {
    if (!(left instanceof DruidQueryRel) || !(right instanceof DruidQueryRel)) {
      // no meaning to filter in broker just use cross join (todo)
      return null;
    }
    if (!(condition instanceof RexCall)) {
      return null;
    }
    final String opName = Utils.opName(condition);
    final List<RexNode> operands = ((RexCall) condition).getOperands();
    if (operands.size() == 2 && SUPPORTED.contains(opName)) {
      int[] refs = Utils.getInputRefs(operands);
      if (refs == null) {
        return null;
      }
      final int shift = left.getRowType().getFieldList().size();
      if (opName.endsWith("CONTAINS")) {
        // B JOIN P ST_CONTAINS(b, p) --> P + B, flip
        // P JOIN B ST_CONTAINS(b, p) --> P + B
        if (refs[0] < shift && refs[1] >= shift) {
          return create(right, left, shift(builder, operands.get(0), shift), operands.get(1), true);
        } else if (refs[0] >= shift && refs[1] < shift) {
          return create(left, right, operands.get(1), shift(builder, operands.get(0), shift), false);
        }
      } else {
        // P JOIN B ST_WITHIN(p, b) --> P + B
        // B JOIN P ST_WITHIN(p, b) --> P + B, flip
        if (refs[0] < shift && refs[1] >= shift) {
          return create(left, right, operands.get(0), shift(builder, operands.get(1), shift), false);
        } else if (refs[0] >= shift && refs[1] < shift) {
          return create(right, left, shift(builder, operands.get(1), shift), operands.get(0), true);
        }
      }
    }
    return null;
  }

  private static RexNode shift(RexBuilder builder, final RexNode node, final int offset)
  {
    return node.accept(new RexShuttle()
    {
      @Override
      public RexNode visitInputRef(RexInputRef ref)
      {
        return builder.makeInputRef(ref.getType(), ref.getIndex() - offset);
      }
    });
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
