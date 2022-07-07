/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.sql.calcite.planner.func;

import io.druid.sql.calcite.planner.SqlStdOperatorTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public class SqlSubstringFunction extends SqlFunction
{
  private static final SqlFunction ORG = org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING;

  public SqlSubstringFunction()
  {
    super(
        ORG.getName(),
        ORG.getKind(),
        ORG.getReturnTypeInference(),
        SqlStdOperatorTable.explicit(VARCHAR, INTEGER, INTEGER),
        ORG.getOperandTypeChecker(),
        ORG.getFunctionType()
    );
  }

  @Override
  public String getSignatureTemplate(final int operandsCount)
  {
    return ORG.getSignatureTemplate(operandsCount);
  }

  @Override
  public String getAllowedSignatures(String opName)
  {
    return ORG.getAllowedSignatures(opName);
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
  {
    return ORG.checkOperandTypes(callBinding, throwOnFailure);
  }

  @Override
  public SqlOperandCountRange getOperandCountRange()
  {
    return ORG.getOperandCountRange();
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec)
  {
    ORG.unparse(writer, call, leftPrec, rightPrec);
  }

  @Override
  public SqlMonotonicity getMonotonicity(SqlOperatorBinding call)
  {
    return ORG.getMonotonicity(call);
  }
}
