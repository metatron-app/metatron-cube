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

import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.KeyedData.StringKeyed;
import io.druid.sql.calcite.Utils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class ExtractCommonFromDisjunction extends RelOptRule
{
  public static final ExtractCommonFromDisjunction INSTANCE = new ExtractCommonFromDisjunction();

  private ExtractCommonFromDisjunction()
  {
    super(operand(LogicalFilter.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final LogicalFilter filter = call.rel(0);
    final RexBuilder builder = filter.getCluster().getRexBuilder();
    final RexNode converted = extract(filter.getCondition(), builder);
    if (converted != filter.getCondition()) {
      call.transformTo(LogicalFilter.create(filter.getInput(), converted));
    }
  }

  // I'm not sure of this
  // for tpch-19 : OR ( AND(A, B), AND(A, C), AND(A, D)) --> AND (A, OR (B, C, D))
  static RexNode extract(RexNode condition, RexBuilder builder)
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
  private static List<StringKeyed<RexNode>> getAndedNodes(RexNode op)
  {
    if (Utils.isAnd(op)) {
      return StringKeyed.of(((RexCall) op).getOperands());
    } else {
      return Lists.newArrayList(StringKeyed.of(op));
    }
  }
}
