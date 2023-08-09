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

package io.druid.sql.calcite.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

// hack for dynamic param
public class DynamicBinaryOperator extends SqlBinaryOperator
{
  public static DynamicBinaryOperator wrap(SqlBinaryOperator operator)
  {
    return wrap(operator, true);
  }

  public static DynamicBinaryOperator wrap(SqlBinaryOperator operator, boolean leftAssoc)
  {
    return new DynamicBinaryOperator(
        operator.getName(),
        operator.getKind(),
        leftAssoc ? operator.getLeftPrec() : operator.getRightPrec(),
        leftAssoc,
        operator.getReturnTypeInference(),
        operator.getOperandTypeInference(),
        operator.getOperandTypeChecker()
    );
  }

  private static final RelDataType ANY_TYPE = new BasicSqlType(DruidTypeSystem.INSTANCE, SqlTypeName.ANY);

  /**
   * Creates a SqlBinaryOperator.
   *
   * @param name                 Name of operator
   * @param kind                 Kind
   * @param prec                 Precedence
   * @param leftAssoc            Left-associativity
   * @param returnTypeInference  Strategy to infer return type
   * @param operandTypeInference Strategy to infer operand types
   * @param operandTypeChecker   Validator for operand types
   */
  private DynamicBinaryOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean leftAssoc,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker
  )
  {
    super(name, kind, prec, leftAssoc, returnTypeInference, operandTypeInference, operandTypeChecker);
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call)
  {
    for (SqlNode node : call.getOperandList()) {
      if (node instanceof SqlDynamicParam) {
        validator.setValidatedNodeType(node, ANY_TYPE);   // skip validation error
      }
    }
    return super.deriveType(validator, scope, call);
  }
}
