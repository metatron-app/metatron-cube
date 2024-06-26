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

import io.druid.java.util.common.IAE;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

public class AliasedOperatorConversion implements SqlOperatorConversion
{
  private final SqlOperatorConversion baseConversion;
  private final SqlOperator operator;

  public AliasedOperatorConversion(SqlOperatorConversion baseConversion, String name)
  {
    if (!SqlKind.FUNCTION.contains(baseConversion.calciteOperator().getKind())) {
      throw new IAE("Base operator must be a function but was[%s]", baseConversion.calciteOperator().getKind());
    }

    SqlFunction baseFunction = (SqlFunction) baseConversion.calciteOperator();

    this.baseConversion = baseConversion;
    this.operator = new SqlFunction(
        name,
        baseFunction.getKind(),
        baseFunction.getReturnTypeInference(),
        baseFunction.getOperandTypeInference(),
        baseFunction.getOperandTypeChecker(),
        baseFunction.getFunctionType()
    );
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Override
  public DruidExpression toDruidExpression(PlannerContext context, RowSignature signature, RexNode rexNode)
  {
    return baseConversion.toDruidExpression(context, signature, rexNode);
  }
}
