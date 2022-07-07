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
import com.google.common.collect.ImmutableMap;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.granularity.PeriodGranularity;
import io.druid.java.util.common.ISE;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.Period;

import java.util.Map;

public class CastOperatorConversion implements SqlOperatorConversion
{
  private static final Map<SqlTypeName, ValueDesc> EXPRESSION_TYPES;

  static {
    final ImmutableMap.Builder<SqlTypeName, ValueDesc> builder = ImmutableMap.builder();

    builder.put(SqlTypeName.BOOLEAN, ValueDesc.BOOLEAN);
    builder.put(SqlTypeName.FLOAT, ValueDesc.FLOAT);
    builder.put(SqlTypeName.REAL, ValueDesc.FLOAT);
    builder.put(SqlTypeName.DOUBLE, ValueDesc.DOUBLE);
    builder.put(SqlTypeName.DECIMAL, ValueDesc.DECIMAL);

    for (SqlTypeName type : SqlTypeName.INT_TYPES) {
      builder.put(type, ValueDesc.LONG);
    }

    for (SqlTypeName type : SqlTypeName.STRING_TYPES) {
      builder.put(type, ValueDesc.STRING);
    }

    // Timestamps are treated as longs (millis since the epoch) in Druid expressions.
    builder.put(SqlTypeName.TIMESTAMP, ValueDesc.LONG);
    builder.put(SqlTypeName.DATE, ValueDesc.LONG);
    builder.put(SqlTypeName.ANY, ValueDesc.UNKNOWN);

    EXPRESSION_TYPES = builder.build();
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return SqlStdOperatorTable.CAST;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final RexNode operand = ((RexCall) rexNode).getOperands().get(0);
    final DruidExpression operandExpression = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        operand
    );

    if (operandExpression == null) {
      return null;
    }

    final SqlTypeName fromType = Calcites.getTypeName(operand.getType());
    final SqlTypeName toType = Calcites.getTypeName(rexNode.getType());

    if (SqlTypeName.CHAR_TYPES.contains(fromType) && SqlTypeName.DATETIME_TYPES.contains(toType)) {
      return castCharToDateTime(plannerContext, operandExpression, toType, rowSignature);
    } else if (SqlTypeName.DATETIME_TYPES.contains(fromType) && SqlTypeName.CHAR_TYPES.contains(toType)) {
      return castDateTimeToChar(plannerContext, operandExpression, fromType);
    } else {
      // Handle other casts.
      final ValueDesc fromExprType = EXPRESSION_TYPES.get(fromType);
      final ValueDesc toExprType = EXPRESSION_TYPES.get(toType);

      if (fromExprType == null || toExprType == null) {
        // We have no runtime type for these SQL types.
        return null;
      }

      final DruidExpression typeCastExpression;

      if (!fromExprType.equals(toExprType)) {
        // Ignore casts for simple extractions (use Function.identity) since it is ok in many cases.
        typeCastExpression = operandExpression.map(
            simpleExtraction -> null,
            expression -> StringUtils.format(
                "CAST(%s, '%s')", expression, toExprType.isPrimitive() ? toExprType.type() : toExprType.toString()
            )
        );
      } else {
        typeCastExpression = operandExpression;
      }

      if (toType == SqlTypeName.DATE) {
        // Floor to day when casting to DATE.
        return TimeFloorOperatorConversion.applyTimeFloor(
            typeCastExpression,
            new PeriodGranularity(Period.days(1), null, plannerContext.getTimeZone()),
            rowSignature
        );
      } else {
        return typeCastExpression;
      }
    }
  }

  private static DruidExpression castCharToDateTime(
      final PlannerContext plannerContext,
      final DruidExpression operand,
      final SqlTypeName toType,
      final RowSignature signature
  )
  {
    // Cast strings to datetimes by parsing them from SQL format.
    final DruidExpression timestampExpression = DruidExpression.fromFunctionCall(
        "timestamp_parse",
        ImmutableList.of(
            operand,
            DruidExpression.fromExpression(DruidExpression.nullLiteral()),
            DruidExpression.fromExpression(DruidExpression.stringLiteral(plannerContext.getTimeZone().getID()))
        )
    );

    if (toType == SqlTypeName.DATE) {
      return TimeFloorOperatorConversion.applyTimeFloor(
          timestampExpression,
          new PeriodGranularity(Period.days(1), null, plannerContext.getTimeZone()),
          signature
      );
    } else if (toType == SqlTypeName.TIMESTAMP) {
      return timestampExpression;
    } else {
      throw new ISE("Unsupported DateTime type[%s]", toType);
    }
  }

  private static DruidExpression castDateTimeToChar(
      final PlannerContext plannerContext,
      final DruidExpression operand,
      final SqlTypeName fromType
  )
  {
    return DruidExpression.fromFunctionCall(
        "timestamp_format",
        ImmutableList.of(
            operand,
            DruidExpression.fromExpression(DruidExpression.stringLiteral(dateTimeFormatString(fromType))),
            DruidExpression.fromExpression(DruidExpression.stringLiteral(plannerContext.getTimeZone().getID()))
        )
    );
  }

  private static String dateTimeFormatString(final SqlTypeName sqlTypeName)
  {
    if (sqlTypeName == SqlTypeName.DATE) {
      return "yyyy-MM-dd";
    } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
      return "yyyy-MM-dd HH:mm:ss";
    } else {
      throw new ISE("Unsupported DateTime type[%s]", sqlTypeName);
    }
  }
}
