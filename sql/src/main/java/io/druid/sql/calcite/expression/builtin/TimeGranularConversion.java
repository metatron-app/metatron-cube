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
import io.druid.granularity.PeriodGranularity;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.util.List;
import java.util.Objects;

public abstract class TimeGranularConversion implements SqlOperatorConversion
{
  public static DruidExpression applyTimeFloor(
      DruidExpression input, PeriodGranularity granularity, RowSignature signature
  )
  {
    return applyTimeGran("timestamp_floor", input, granularity, signature);
  }

  public static DruidExpression applyTimeGran(
      final String functionName,
      final DruidExpression input,
      final PeriodGranularity granularity,
      final RowSignature signature
  )
  {
    Preconditions.checkNotNull(input, "input");
    Preconditions.checkNotNull(granularity, "granularity");

    // Collapse floor chains if possible. Useful for constructs like CAST(FLOOR(__time TO QUARTER) AS DATE).
    if (granularity.getPeriod().equals(Period.days(1))) {
      final PeriodGranularity inputGranularity = Expressions.asGranularity(input, signature);

      if (inputGranularity != null) {
        if (Objects.equals(inputGranularity.getTimeZone(), granularity.getTimeZone())
            && Objects.equals(inputGranularity.getOrigin(), granularity.getOrigin())
            && periodIsDayMultiple(inputGranularity.getPeriod())) {
          return input;
        }
      }
    }

    Long origin = granularity.getOrigin() == null ? null : granularity.getOrigin().getMillis();
    return DruidExpression.fromFunctionCall(
        functionName,
        input.getExpression(),
        DruidExpression.stringLiteral(granularity.getPeriod().toString()),
        DruidExpression.numberLiteral(origin, SqlTypeName.BIGINT),
        DruidExpression.stringLiteral(granularity.getTimeZone().toString())
    );
  }

  private static boolean periodIsDayMultiple(final Period period)
  {
    return period.getMillis() == 0
           && period.getSeconds() == 0
           && period.getMinutes() == 0
           && period.getHours() == 0
           && (period.getDays() > 0 || period.getWeeks() > 0 || period.getMonths() > 0 || period.getYears() > 0);
  }

  protected DruidExpression toDruidExpression(
      final String functionName,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final List<RexNode> operands = Utils.operands(rexNode);
    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        operands
    );
    if (druidExpressions == null) {
      return null;
    } else if (operands.get(1).isA(SqlKind.LITERAL)
               && (operands.size() <= 2 || operands.get(2).isA(SqlKind.LITERAL))
               && (operands.size() <= 3 || operands.get(3).isA(SqlKind.LITERAL))) {
      // Granularity is a literal. Special case since we can use an extractionFn here.
      final Period period = new Period(RexLiteral.stringValue(operands.get(1)));
      final DateTime origin =
          operands.size() > 2 && !RexLiteral.isNullLiteral(operands.get(2))
          ? Calcites.calciteDateTimeLiteralToJoda(operands.get(2), plannerContext.getTimeZone())
          : null;
      final DateTimeZone timeZone =
          operands.size() > 3 && !RexLiteral.isNullLiteral(operands.get(3))
          ? DateTimeZone.forID(RexLiteral.stringValue(operands.get(3)))
          : plannerContext.getTimeZone();
      final PeriodGranularity granularity = new PeriodGranularity(period, origin, timeZone);
      return applyTimeGran(functionName, druidExpressions.get(0), granularity, rowSignature);
    } else {
      // Granularity is dynamic
      return DruidExpression.fromFunctionCall(functionName, druidExpressions);
    }
  }
}
