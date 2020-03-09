/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.sql.calcite.planner;

import io.druid.common.DateTimes;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.List;

/**
 * A Calcite {@code RexExecutor} that reduces Calcite expressions by evaluating them using Druid's own built-in
 * expressions. This ensures that constant reduction is done in a manner consistent with the query runtime.
 */
public class DruidRexExecutor implements RexExecutor
{
  private final PlannerContext plannerContext;

  public DruidRexExecutor(final PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public void reduce(
      final RexBuilder rexBuilder,
      final List<RexNode> constExps,
      final List<RexNode> reducedValues
  )
  {
    for (RexNode constExp : constExps) {
      final DruidExpression druidExpression = Expressions.toDruidExpression(
          plannerContext,
          Utils.EMPTY_ROW_SIGNATURE,
          constExp
      );

      if (druidExpression == null) {
        reducedValues.add(constExp);
      } else {
        final SqlTypeName sqlTypeName = constExp.getType().getSqlTypeName();
        final Expr expr = Parser.parse(druidExpression.getExpression(), Utils.EMPTY_ROW_SIGNATURE);

        final ExprEval exprResult = Evals.getConstantEval(expr);

        final RexNode literal;

        if (sqlTypeName == SqlTypeName.BOOLEAN) {
          literal = rexBuilder.makeLiteral(exprResult.asBoolean(), constExp.getType(), true);
        } else if (sqlTypeName == SqlTypeName.DATE) {
          literal = rexBuilder.makeDateLiteral(
              Calcites.jodaToCalciteDateString(
                  DateTimes.utc(exprResult.asLong()),
                  plannerContext.getTimeZone()
              )
          );
        } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
          literal = rexBuilder.makeTimestampLiteral(
              Calcites.jodaToCalciteTimestampString(
                  DateTimes.utc(exprResult.asLong()),
                  plannerContext.getTimeZone()
              ),
              RelDataType.PRECISION_NOT_SPECIFIED
          );
        } else if (SqlTypeName.NUMERIC_TYPES.contains(sqlTypeName)) {
          final BigDecimal bigDecimal;

          if (exprResult.type().isDecimal()) {
            bigDecimal = (BigDecimal) exprResult.value();
          } else if (exprResult.type().isLong()) {
            bigDecimal = BigDecimal.valueOf(exprResult.asLong());
          } else {
            bigDecimal = BigDecimal.valueOf(exprResult.asDouble());
          }

          literal = rexBuilder.makeLiteral(bigDecimal, constExp.getType(), true);
        } else if (!ValueDesc.isGeometry(exprResult.type()) &&
                   !ValueDesc.isOGCGeometry(exprResult.type())) {
          // hack.. skip geometries
          literal = rexBuilder.makeLiteral(exprResult.value(), constExp.getType(), true);
        } else {
          literal = constExp;
        }

        reducedValues.add(literal);
      }
    }
  }
}
