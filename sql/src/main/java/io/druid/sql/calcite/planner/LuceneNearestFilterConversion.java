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

import io.druid.query.filter.DimFilter;
import io.druid.query.filter.LuceneNearestFilter;
import io.druid.sql.calcite.expression.DimFilterConversion;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class LuceneNearestFilterConversion implements DimFilterConversion
{
  @Override
  public String name()
  {
    return "lucene_nearest";
  }

  @Override
  public DimFilter toDruidFilter(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    final RexCall call = (RexCall) rexNode;
    final List<RexNode> operands = call.getOperands();
    if (operands.size() != 4 && operands.size() != 5) {
      return null;
    }
    String field = DruidOperatorTable.getFieldName(operands.get(0), plannerContext, rowSignature);
    double latitude = ((Number) RexLiteral.value(operands.get(1))).doubleValue();
    double longitude = ((Number) RexLiteral.value(operands.get(2))).doubleValue();
    int count = RexLiteral.intValue(operands.get(3));
    String scoreField = operands.size() == 5 ? RexLiteral.stringValue(operands.get(4)) : null;
    return new LuceneNearestFilter(field, latitude, longitude, count, scoreField);
  }

  @Override
  public String toString()
  {
    return "LuceneNearestFilterConversion";
  }
}
