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

package io.druid.sql.calcite.expression.builtin;

import com.google.common.collect.ImmutableList;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.planner.SqlStdOperatorTable;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlTrimFunction;

import javax.annotation.Nullable;

public class TrimOperatorConversion implements SqlOperatorConversion
{
  @Nullable
  public static DruidExpression makeTrimExpression(SqlTrimFunction.Flag trimStyle, String... operands)
  {
    final String functionName;

    switch (trimStyle) {
      case LEADING:
        functionName = "ltrim";
        break;
      case TRAILING:
        functionName = "rtrim";
        break;
      case BOTH:
        functionName = "btrim";
        break;
      default:
        // Not reached
        throw new UnsupportedOperationException();
    }

    // Druid version of trim is multi-function (ltrim/rtrim/trim) and the other two args are swapped.
    return DruidExpression.fromFunctionCall(functionName, operands);
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return SqlStdOperatorTable.TRIM;
  }

  @Override
  public DruidExpression toDruidExpression(PlannerContext context, RowSignature signature, RexNode rexNode)
  {
    // TRIM(<style> <chars> FROM <arg>)

    final ImmutableList<RexNode> operands = Utils.operands(rexNode);
    final RexLiteral flag = (RexLiteral) operands.get(0);
    final SqlTrimFunction.Flag trimStyle = (SqlTrimFunction.Flag) flag.getValue();

    final DruidExpression charsExpression = Expressions.toDruidExpression(context, signature, operands.get(1));
    final DruidExpression stringExpression = Expressions.toDruidExpression(context, signature, operands.get(2));

    if (charsExpression == null || stringExpression == null) {
      return null;
    }

    return makeTrimExpression(trimStyle, stringExpression.getExpression(), charsExpression.getExpression());
  }
}
