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

package io.druid.query.filter;

import io.druid.common.guava.GuavaUtils;
import io.druid.segment.lucene.ShapeFormat;
import io.druid.sql.calcite.expression.DimFilterConversion;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class LuceneShapeFilterConversion implements DimFilterConversion
{
  @Override
  public String name()
  {
    return "lucene_shape";
  }

  @Override
  public DimFilter toDruidFilter(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    final RexCall call = (RexCall) rexNode;
    final List<RexNode> operands = call.getOperands();
    if (operands.size() != 2 && operands.size() != 3) {
      return null;
    }
    String field = DruidOperatorTable.getFieldName(operands.get(0), plannerContext, rowSignature);
    ShapeFormat format = operands.size() == 3 ? ShapeFormat.valueOf(RexLiteral.stringValue(operands.get(1))) : null;
    String shape = RexLiteral.stringValue(GuavaUtils.lastOf(operands));
    return new LuceneShapeFilter(field, null, format, shape);
  }
}
