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

import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.OperatorConversions;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

public class LPadOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("LPAD")
      .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER)
      .returnType(SqlTypeName.VARCHAR)
      .functionCategory(SqlFunctionCategory.STRING)
      .requiredOperands(2)
      .build();

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Override
  public DruidExpression toDruidExpression(PlannerContext context, RowSignature signature, RexNode rexNode)
  {
    return OperatorConversions.convertCall(
        context,
        signature,
        rexNode,
        druidExpressions -> {
          if (druidExpressions.size() > 2) {
            return DruidExpression.fromFunctionCall(
                "lpad",
                druidExpressions.get(0),
                druidExpressions.get(1),
                druidExpressions.get(2)
            );
          } else {
            return DruidExpression.fromFunctionCall(
                "lpad",
                druidExpressions.get(0).getExpression(),
                druidExpressions.get(1).getExpression(),
                DruidExpression.stringLiteral(" ")
            );
          }
        }
    );
  }
}
