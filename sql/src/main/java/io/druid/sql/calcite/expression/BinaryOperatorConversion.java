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

package io.druid.sql.calcite.expression;

import com.google.common.base.Joiner;
import io.druid.common.utils.StringUtils;
import io.druid.java.util.common.ISE;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BinaryOperatorConversion implements SqlOperatorConversion
{
  private final SqlOperator operator;
  private final Joiner joiner;

  public BinaryOperatorConversion(final SqlOperator operator, final String druidOperator)
  {
    this.operator = operator;
    this.joiner = Joiner.on(" " + druidOperator + " ");
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Override
  public DruidExpression toDruidExpression(PlannerContext context, RowSignature signature, RexNode rexNode)
  {
    List<RexNode> operands = Utils.operands(rexNode);
    RexNode op1 = operands.get(0);
    RexNode op2 = operands.get(1);
    if (Calcites.isFloatCastedToDouble(op1) && Calcites.isLiteralDouble(op2)) {
      DruidExpression expression = Expressions.toDruidExpression(
          context, signature, Utils.operands(op1).get(0)
      );
      DruidExpression constant = DruidExpression.fromNumericLiteral((Number) RexLiteral.value(op2), SqlTypeName.FLOAT);
      if (expression != null && constant != null) {
        return opExpression(Arrays.asList(expression, constant));
      }
    } else if (Calcites.isLiteralDouble(op1) && Calcites.isFloatCastedToDouble(op2)) {
      DruidExpression constant = DruidExpression.fromNumericLiteral((Number) RexLiteral.value(op1), SqlTypeName.FLOAT);
      DruidExpression expression = Expressions.toDruidExpression(
          context, signature, Utils.operands(op2).get(0)
      );
      if (expression != null && constant != null) {
        return opExpression(Arrays.asList(constant, expression));
      }
    }
    return OperatorConversions.convertCall(context, signature, rexNode, this::opExpression);
  }

  private DruidExpression opExpression(List<DruidExpression> operands)
  {
    if (operands.size() < 2) {
      throw new ISE("Got binary operator[%s] with %s args?", operator.getName(), operands.size());
    }
    return DruidExpression.fromExpression(
        StringUtils.format(
            "(%s)",
            joiner.join(
                operands.stream()
                        .map(DruidExpression::getExpression)
                        .collect(Collectors.toList())
            )
        )
    );
  }
}
