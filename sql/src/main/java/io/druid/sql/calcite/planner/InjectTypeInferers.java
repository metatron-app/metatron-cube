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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

class InjectTypeInferers extends SqlFunction
{
  protected final SqlFunction delegate;

  InjectTypeInferers(SqlFunction delegate, SqlReturnTypeInference retType, SqlOperandTypeInference opTypes)
  {
    super(
        delegate.getName(),
        delegate.getKind(),
        retType == null ? delegate.getReturnTypeInference() : retType,
        opTypes == null ? delegate.getOperandTypeInference() : opTypes,
        delegate.getOperandTypeChecker(),
        delegate.getFunctionType()
    );
    this.delegate = delegate;
  }

  @Override
  public final SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands)
  {
    SqlCall call = delegate.createCall(functionQualifier, pos, operands);
    return new SqlBasicCall(this, call.getOperandList().toArray(new SqlNode[0]), call.getParserPosition());
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec)
  {
    delegate.unparse(writer, call, leftPrec, rightPrec);
  }

  @Override
  public String getSignatureTemplate(final int operandsCount)
  {
    return delegate.getSignatureTemplate(operandsCount);
  }

  @Override
  public String getAllowedSignatures(String opName)
  {
    return delegate.getAllowedSignatures(opName);
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure)
  {
    return delegate.checkOperandTypes(callBinding, throwOnFailure);
  }

  @Override
  public SqlOperandCountRange getOperandCountRange()
  {
    return delegate.getOperandCountRange();
  }

  @Override
  public SqlMonotonicity getMonotonicity(SqlOperatorBinding call)
  {
    return delegate.getMonotonicity(call);
  }
}
