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

import io.druid.common.utils.StringUtils;
import io.druid.granularity.PeriodGranularity;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.expression.TimeUnits;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public abstract class GranularConversion implements SqlOperatorConversion
{
  protected DruidExpression toDruidExpression(
      final String unary,
      final String binary,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final RexNode arg = call.getOperands().get(0);
    final DruidExpression druidExpression = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        arg
    );
    if (druidExpression == null) {
      return null;
    } else if (call.getOperands().size() == 1) {
      // func(expr)
      return druidExpression.map(
          simpleExtraction -> null,
          expression -> StringUtils.format("%s(%s)", unary, expression)
      );
    } else if (call.getOperands().size() == 2) {
      // func(expr TO timeUnit)
      final RexLiteral flag = (RexLiteral) call.getOperands().get(1);
      final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
      final PeriodGranularity granularity = TimeUnits.toQueryGranularity(timeUnit, plannerContext.getTimeZone());
      if (granularity == null) {
        return null;
      }

      return TimeFloorOperatorConversion.applyTimeGran(binary, druidExpression, granularity, rowSignature);
    } else {
      // CEIL with 3 arguments?
      return null;
    }
  }
}
