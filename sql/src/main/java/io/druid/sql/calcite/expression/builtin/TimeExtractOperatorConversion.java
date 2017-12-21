/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.expression.builtin;

import com.google.common.collect.ImmutableMap;
import io.druid.common.utils.StringUtils;
import io.druid.math.expr.DateTimeFunctions;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.OperatorConversions;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTimeZone;

import java.util.Map;

public class TimeExtractOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("TIME_EXTRACT")
      .operandTypes(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
      .requiredOperands(1)
      .returnType(SqlTypeName.BIGINT)
      .functionCategory(SqlFunctionCategory.TIMEDATE)
      .build();

  // Note that QUARTER is not supported here.
  private static final Map<DateTimeFunctions.Unit, String> EXTRACT_FORMAT_MAP =
      ImmutableMap.<DateTimeFunctions.Unit, String>builder()
          .put(DateTimeFunctions.Unit.SECOND, "s")
          .put(DateTimeFunctions.Unit.MINUTE, "m")
          .put(DateTimeFunctions.Unit.HOUR, "H")
          .put(DateTimeFunctions.Unit.DAY, "d")
          .put(DateTimeFunctions.Unit.DOW, "e")
          .put(DateTimeFunctions.Unit.DOY, "D")
          .put(DateTimeFunctions.Unit.WEEK, "w")
          .put(DateTimeFunctions.Unit.MONTH, "M")
          .put(DateTimeFunctions.Unit.YEAR, "Y")
          .build();

  public static DruidExpression applyTimeExtract(
      final DruidExpression timeExpression,
      final DateTimeFunctions.Unit unit,
      final DateTimeZone timeZone
  )
  {
    return timeExpression.map(
        simpleExtraction -> {
          final String formatString = EXTRACT_FORMAT_MAP.get(unit);
          if (formatString == null) {
            return null;
          } else {
            return TimeFormatOperatorConversion.applyTimestampFormat(
                simpleExtraction,
                formatString,
                timeZone
            );
          }
        },
        expression -> StringUtils.format(
            "datetime_extract(%s,%s,%s)",
            DruidExpression.stringLiteral(unit.name()),
            expression,
            DruidExpression.stringLiteral(timeZone.getID())
        )
    );
  }

  @Override
  public SqlFunction calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final RexNode timeArg = call.getOperands().get(0);
    final DruidExpression timeExpression = Expressions.toDruidExpression(plannerContext, rowSignature, timeArg);
    if (timeExpression == null) {
      return null;
    }

    final DateTimeFunctions.Unit unit = DateTimeFunctions.Unit.valueOf(
        StringUtils.toUpperCase(RexLiteral.stringValue(call.getOperands().get(1)))
    );

    final DateTimeZone timeZone = call.getOperands().size() > 2 && !RexLiteral.isNullLiteral(call.getOperands().get(2))
                                  ? DateTimeZone.forID(RexLiteral.stringValue(call.getOperands().get(2)))
                                  : plannerContext.getTimeZone();

    return applyTimeExtract(timeExpression, unit, timeZone);
  }
}
