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

import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.OperatorConversions;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class EvalOperatorConversion implements SqlOperatorConversion
{
  public static EvalOperatorConversion of(String name, SqlTypeName sqlType)
  {
    return new EvalOperatorConversion(name, sqlType);
  }

  private final SqlFunction function;

  private EvalOperatorConversion(String name, SqlTypeName sqlType)
  {
    this.function = OperatorConversions
        .operatorBuilder(name)
        .operandTypes(SqlTypeFamily.CHARACTER)
        .returnType(sqlType)
        .functionCategory(SqlFunctionCategory.USER_DEFINED_SPECIFIC_FUNCTION)
        .build();
  }

  @Override
  public SqlFunction calciteOperator()
  {
    return function;
  }

  @Override
  public DruidExpression toDruidExpression(PlannerContext context, RowSignature signature, RexNode rexNode)
  {
    List<RexNode> operands = Utils.operands(rexNode);
    if (operands.size() == 1) {
      return DruidExpression.fromExpression(RexLiteral.stringValue(operands.get(0)));
    }
    return null;
  }
}
