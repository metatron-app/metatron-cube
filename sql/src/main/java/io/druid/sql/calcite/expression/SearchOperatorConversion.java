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

import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.planner.DruidTypeSystem;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.planner.SqlStdOperatorTable;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

public class SearchOperatorConversion implements SqlOperatorConversion
{
  private static final RexBuilder REX_BUILDER = new RexBuilder(new JavaTypeFactoryImpl(DruidTypeSystem.INSTANCE));

  @Override
  public SqlOperator calciteOperator()
  {
    return SqlStdOperatorTable.SEARCH;
  }

  @Override
  public DruidExpression toDruidExpression(PlannerContext context, RowSignature signature, RexNode rexNode)
  {
    return Expressions.toDruidExpression(context, signature, Utils.expand(REX_BUILDER, rexNode));
  }
}