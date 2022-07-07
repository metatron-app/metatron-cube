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

import io.druid.sql.calcite.planner.func.SqlSubstringFunction;
import io.druid.sql.calcite.planner.func.SqlTrimFunction;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

public class SqlStdOperatorTable extends org.apache.calcite.sql.fun.SqlStdOperatorTable
{
  public static final SqlFunction SUBSTRING = new SqlSubstringFunction();

  public static final SqlFunction TRIM = new SqlTrimFunction();

  private static SqlOperandTypeInference explicit(List<RelDataType> dataTypes)
  {
    return (callBinding, returnType, operandTypes) -> {
      RelDataType unknownType = callBinding.getValidator().getUnknownType();
      for (int i = 0; i < Math.min(dataTypes.size(), operandTypes.length); i++) {
        if (unknownType.equals(operandTypes[i])) {
          operandTypes[i] = dataTypes.get(i);
        }
      }
    };
  }

  public static SqlOperandTypeInference explicit(SqlTypeName... typeNames)
  {
    return (callBinding, returnType, operandTypes) -> {
      List<SqlNode> operands = callBinding.operands();
      SqlValidator validator = callBinding.getValidator();
      RelDataType unknownType = validator.getUnknownType();
      RelDataTypeFactory factory = callBinding.getTypeFactory();
      for (int i = 0; i < Math.min(typeNames.length, operandTypes.length); i++) {
        if (unknownType.equals(operandTypes[i])) {
          RelDataType type = validator.deriveType(callBinding.getScope(), operands.get(i));
          if (unknownType.equals(type) && typeNames[i] != null) {
            type = factory.createSqlType(typeNames[i]);
          }
          operandTypes[i] = type;
        }
      }
    };
  }
}
