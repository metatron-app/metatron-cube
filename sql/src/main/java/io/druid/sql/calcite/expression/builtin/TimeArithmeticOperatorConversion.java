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

import com.google.common.base.Preconditions;
import io.druid.common.utils.StringUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.util.List;

/**
 * Base class for a number of time arithmetic related operators.
 */
public abstract class TimeArithmeticOperatorConversion implements SqlOperatorConversion
{
  private final SqlOperator operator;
  private final int direction;

  public TimeArithmeticOperatorConversion(final SqlOperator operator, final int direction)
  {
    this.operator = operator;
    this.direction = direction;
    Preconditions.checkArgument(direction != 0);
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Override
  public DruidExpression toDruidExpression(PlannerContext context, RowSignature signature, RexNode rexNode)
  {
    final List<RexNode> operands = Utils.operands(rexNode);
    if (operands.size() != 2) {
      throw new IAE("Expected 2 args, got %s", operands.size());
    }

    final RexNode timeRexNode = operands.get(0);
    final RexNode shiftRexNode = operands.get(1);

    final DruidExpression timeExpr = Expressions.toDruidExpression(context, signature, timeRexNode);
    final DruidExpression shiftExpr = Expressions.toDruidExpression(context, signature, shiftRexNode);

    if (timeExpr == null || shiftExpr == null) {
      return null;
    }

    if (shiftRexNode.getType().getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH) {
      // timestamp_expr { + | - } <interval_expr> (year-month interval)
      // Period is a value in months.
      return DruidExpression.fromExpression(
          DruidExpression.functionCall(
              "timestamp_shift",
              timeExpr,
              shiftExpr.map(expression -> StringUtils.format("'P%sM'", expression)),
              DruidExpression.numberLiteral(direction > 0 ? 1 : -1)
          )
      );
    } else if (shiftRexNode.getType().getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME) {
      // timestamp_expr { + | - } <interval_expr> (day-time interval)
      // Period is a value in milliseconds. Ignore time zone.
      return DruidExpression.fromExpression(
          StringUtils.format(
              "(%s %s %s)",
              timeExpr.getExpression(),
              direction > 0 ? "+" : "-",
              shiftExpr.getExpression()
          )
      );
    } else {
      // Shouldn't happen if subclasses are behaving.
      throw new ISE("Got unexpected type period type family[%s]", shiftRexNode.getType().getFamily());
    }
  }

  public static class TimePlusIntervalOperatorConversion extends TimeArithmeticOperatorConversion
  {
    public TimePlusIntervalOperatorConversion()
    {
      super(SqlStdOperatorTable.DATETIME_PLUS, 1);
    }
  }

  public static class TimeMinusIntervalOperatorConversion extends TimeArithmeticOperatorConversion
  {
    public TimeMinusIntervalOperatorConversion()
    {
      super(SqlStdOperatorTable.MINUS_DATE, -1);
    }
  }
}
