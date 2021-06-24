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

package io.druid.sql.calcite.expression;

import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class BitSetUnwrapConversion implements SqlOperatorConversion
{
  private final SqlFunction function;

  public BitSetUnwrapConversion()
  {
    this.function = OperatorConversions
        .operatorBuilder("unwrap")
        .operandTypes(SqlTypeFamily.ARRAY)
        .returnTypeInference(opBinding -> {
          if (opBinding.getOperandType(0).getComponentType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            RelDataTypeFactory factory = opBinding.getTypeFactory();
            return factory.createTypeWithNullability(
                factory.createArrayType(factory.createSqlType(SqlTypeName.INTEGER), -1), true
            );
          }
          return null;
        })
        .functionCategory(SqlFunctionCategory.USER_DEFINED_SPECIFIC_FUNCTION)
        .build();
  }

  @Override
  public SqlFunction calciteOperator()
  {
    return function;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final List<RexNode> operands = call.getOperands();
    if (operands.size() == 1 && operands.get(0).getKind() == SqlKind.INPUT_REF) {
      String column = rowSignature.getColumnNames().get(((RexInputRef) operands.get(0)).getIndex());
      return DruidExpression.fromExpression(String.format("bitset.unwrap(%s)", DruidExpression.identifier(column)));
    }
    return null;
  }
}
