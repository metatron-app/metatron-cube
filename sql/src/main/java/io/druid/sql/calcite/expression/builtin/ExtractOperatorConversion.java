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

import com.google.common.collect.ImmutableMap;
import io.druid.math.expr.DateTimeFunctions;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.SqlExtractFunction;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;

import java.util.List;
import java.util.Map;

public class ExtractOperatorConversion implements SqlOperatorConversion
{
  private static final Map<TimeUnitRange, DateTimeFunctions.Unit> EXTRACT_UNIT_MAP =
      ImmutableMap.<TimeUnitRange, DateTimeFunctions.Unit>builder()
          .put(TimeUnitRange.EPOCH, DateTimeFunctions.Unit.EPOCH)
          .put(TimeUnitRange.SECOND, DateTimeFunctions.Unit.SECOND)
          .put(TimeUnitRange.MINUTE, DateTimeFunctions.Unit.MINUTE)
          .put(TimeUnitRange.HOUR, DateTimeFunctions.Unit.HOUR)
          .put(TimeUnitRange.DAY, DateTimeFunctions.Unit.DAY)
          .put(TimeUnitRange.DOW, DateTimeFunctions.Unit.DOW)
          .put(TimeUnitRange.DOY, DateTimeFunctions.Unit.DOY)
          .put(TimeUnitRange.WEEK, DateTimeFunctions.Unit.WEEK)
          .put(TimeUnitRange.MONTH, DateTimeFunctions.Unit.MONTH)
          .put(TimeUnitRange.QUARTER, DateTimeFunctions.Unit.QUARTER)
          .put(TimeUnitRange.YEAR, DateTimeFunctions.Unit.YEAR)
          .build();

  @Override
  public SqlFunction calciteOperator()
  {
    return SqlExtractFunction.INSTANCE;
  }

  @Override
  public DruidExpression toDruidExpression(PlannerContext context, RowSignature signature, RexNode rexNode)
  {
    // EXTRACT(timeUnit FROM arg)
    final List<RexNode> operands = Utils.operands(rexNode);
    final RexLiteral flag = (RexLiteral) operands.get(0);
    final TimeUnitRange calciteUnit = (TimeUnitRange) flag.getValue();
    final RexNode arg = operands.get(1);

    final DruidExpression input = Expressions.toDruidExpression(context, signature, arg);
    if (input == null) {
      return null;
    }

    final DateTimeFunctions.Unit druidUnit = EXTRACT_UNIT_MAP.get(calciteUnit);
    if (druidUnit == null) {
      // Don't know how to extract this time unit.
      return null;
    }

    return TimeExtractOperatorConversion.applyTimeExtract(input, druidUnit, context.getTimeZone());
  }
}
