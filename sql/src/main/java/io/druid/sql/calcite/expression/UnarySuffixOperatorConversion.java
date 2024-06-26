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

import com.google.common.collect.Iterables;
import io.druid.common.utils.StringUtils;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

public class UnarySuffixOperatorConversion implements SqlOperatorConversion
{
  private final SqlOperator operator;
  private final String druidOperator;

  public UnarySuffixOperatorConversion(final SqlOperator operator, final String druidOperator)
  {
    this.operator = operator;
    this.druidOperator = druidOperator;
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Override
  public DruidExpression toDruidExpression(PlannerContext context, RowSignature signature, RexNode rexNode)
  {
    return OperatorConversions.convertCall(
        context,
        signature,
        rexNode,
        operands -> DruidExpression.fromExpression(
            StringUtils.format(
                "(%s %s)",
                Iterables.getOnlyElement(operands).getExpression(),
                druidOperator
            )
        )
    );
  }
}
