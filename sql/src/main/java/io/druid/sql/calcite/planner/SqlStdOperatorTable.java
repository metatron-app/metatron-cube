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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public class SqlStdOperatorTable extends org.apache.calcite.sql.fun.SqlStdOperatorTable
{
  public static final RelProtoDataType P_INTEGER = f -> f.createSqlType(INTEGER);
  public static final RelProtoDataType P_VARCHAR = f -> f.createSqlType(VARCHAR);
  public static final RelProtoDataType P_NVARCHAR = f -> f.createTypeWithNullability(f.createSqlType(VARCHAR), true);

  private static final SqlReturnTypeInference NULLABLE_VARCHAR = ReturnTypes.cascade(
      ReturnTypes.explicit(VARCHAR), SqlTypeTransforms.TO_NULLABLE
  );

  public static final SqlFunction SUBSTRING = new InjectTypeInference(
      org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING,
      NULLABLE_VARCHAR, explicit(P_NVARCHAR, P_INTEGER, P_INTEGER)
  );

  public static final SqlFunction TRIM = new InjectTypeInference(
      org.apache.calcite.sql.fun.SqlStdOperatorTable.TRIM,
      NULLABLE_VARCHAR, explicit(P_NVARCHAR, P_VARCHAR, P_VARCHAR)
  );

  public static final SqlFunction COALESCE = new InjectTypeInference(
      org.apache.calcite.sql.fun.SqlStdOperatorTable.COALESCE, null, InferTypes.FIRST_KNOWN
  );

  public static final SqlFunction REPLACE = new InjectTypeInference(
      org.apache.calcite.sql.fun.SqlStdOperatorTable.REPLACE,
      NULLABLE_VARCHAR, explicit(P_NVARCHAR, P_VARCHAR, P_VARCHAR)
  );

  public static final SqlFunction UPPER = new InjectTypeInference(
      org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER, NULLABLE_VARCHAR, explicit(P_NVARCHAR)
  );

  public static final SqlFunction LOWER = new InjectTypeInference(
      org.apache.calcite.sql.fun.SqlStdOperatorTable.LOWER, NULLABLE_VARCHAR, explicit(P_NVARCHAR)
  );

  public static final SqlFunction INITCAP = new InjectTypeInference(
      org.apache.calcite.sql.fun.SqlStdOperatorTable.INITCAP, NULLABLE_VARCHAR, explicit(P_NVARCHAR)
  );

  private static SqlOperandTypeInference explicit(RelDataType... dataTypes)
  {
    return (callBinding, returnType, operandTypes) -> {
      final RelDataType unknownType = callBinding.getValidator().getUnknownType();
      for (int i = 0; i < Math.min(dataTypes.length, operandTypes.length); i++) {
        if (dataTypes[i] != null && unknownType.equals(operandTypes[i])) {
          operandTypes[i] = dataTypes[i];
        }
      }
    };
  }

  public static SqlOperandTypeInference explicit(RelProtoDataType... typeNames)
  {
    return (callBinding, returnType, operandTypes) -> {
      final RelDataType unknownType = callBinding.getValidator().getUnknownType();
      final RelDataTypeFactory factory = callBinding.getTypeFactory();
      for (int i = 0; i < Math.min(typeNames.length, operandTypes.length); i++) {
        if (typeNames[i] != null && unknownType.equals(operandTypes[i])) {
          operandTypes[i] = typeNames[i].apply(factory);
        }
      }
    };
  }
}
