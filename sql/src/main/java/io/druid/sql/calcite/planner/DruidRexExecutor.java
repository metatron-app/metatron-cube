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

import com.google.common.collect.Lists;
import io.druid.common.DateTimes;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.rel.QueryMaker;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.util.List;

/**
 * A Calcite {@code RexExecutor} that reduces Calcite expressions by evaluating them using Druid's own built-in
 * expressions. This ensures that constant reduction is done in a manner consistent with the query runtime.
 */
public class DruidRexExecutor implements RexExecutor
{
  private final PlannerContext plannerContext;

  public DruidRexExecutor(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues)
  {
    for (RexNode constExp : constExps) {
      reducedValues.add(reduce(rexBuilder, constExp));
    }
  }

  private RexNode reduce(RexBuilder rexBuilder, RexNode rexNode)
  {
    if (rexNode.isA(SqlKind.LITERAL)) {
      return rexNode;
    }
    DruidExpression expression = Expressions.toDruidExpression(plannerContext, Utils.EMPTY_ROW_SIGNATURE, rexNode);
    if (expression != null) {
      Expr expr = Parser.parse(expression.getExpression(), Utils.EMPTY_ROW_SIGNATURE);
      if (Evals.isConstant(expr)) {
        rexNode = constantToLiteral(rexBuilder, rexNode, Evals.getConstantEval(expr));
      }
    } else if (rexNode instanceof RexCall) {
      RexCall call = (RexCall) rexNode;
      List<RexNode> overwrite = null;
      for (int i = 0; i < call.operands.size(); i++) {
        RexNode operand = call.operands.get(i);
        RexNode reduced = reduce(rexBuilder, operand);
        if (reduced != operand) {
          if (overwrite == null) {
            overwrite = Lists.newArrayList(call.operands);
          }
          overwrite.set(i, reduced);
        }
      }
      if (overwrite != null) {
        rexNode = rexBuilder.makeCall(call.op, overwrite);
      }
    }
    return rexNode;
  }

  private RexNode constantToLiteral(RexBuilder rexBuilder, RexNode rexNode, ExprEval eval)
  {
    if (eval.isNull()) {
      return rexBuilder.makeLiteral(null, rexNode.getType(), false);
    }
    if (ValueDesc.isGeometry(eval.type()) || ValueDesc.isOGCGeometry(eval.type())) {
      // hack.. skip geometries
      return rexNode;
    }
    SqlTypeName sqlTypeName = Calcites.getTypeName(rexNode.getType());
    if (sqlTypeName == SqlTypeName.BOOLEAN) {
      return rexBuilder.makeLiteral(eval.asBoolean(), rexNode.getType(), false);
    }
    if (SqlTypeName.NUMERIC_TYPES.contains(sqlTypeName)) {
      return rexBuilder.makeLiteral(toDecimal(eval), rexNode.getType(), false);
    }
    DateTimeZone timeZone = plannerContext.getTimeZone();
    if (sqlTypeName == SqlTypeName.DATE) {
      DateTime timestamp = DateTimes.utc(eval.asLong());
      return rexBuilder.makeDateLiteral(Calcites.jodaToCalciteDateString(timestamp, timeZone));
    }
    if (sqlTypeName == SqlTypeName.TIMESTAMP) {
      DateTime timestamp = DateTimes.utc(eval.asLong());
      return rexBuilder.makeTimestampLiteral(
          Calcites.jodaToCalciteTimestampString(timestamp, timeZone), RelDataType.PRECISION_NOT_SPECIFIED
      );
    }
    Object value = eval.value();
    if (sqlTypeName == SqlTypeName.ARRAY) {
      value = QueryMaker.coerceArray(value);
    }
    return rexBuilder.makeLiteral(value, rexNode.getType(), false);
  }

  private BigDecimal toDecimal(ExprEval eval)
  {
    if (eval.isNull()) {
      return null;
    }
    Object value = eval.value();
    if (value instanceof Long) {
      return BigDecimal.valueOf((Long) value);
    }
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    if (value instanceof Number) {
      return BigDecimal.valueOf(((Number) value).doubleValue());
    }
    return BigDecimal.valueOf(eval.asDouble());
  }
}
