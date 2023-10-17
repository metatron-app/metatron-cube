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

package io.druid.segment.lucene;

import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.DimFilterConversion;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class LuceneKnnVectorConversion implements DimFilterConversion
{
  @Override
  public String name()
  {
    return "lucene_knn_vector";
  }

  @Override
  public DimFilter toDruidFilter(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    List<RexNode> operands = Utils.operands(rexNode);
    if (operands.size() != 3 && operands.size() != 4) {
      return null;
    }
    RexNode operand = operands.get(0);
    String field = DruidOperatorTable.getFieldName(operand, plannerContext, rowSignature);
    float[] vector = Utils.extractFoatArray(operands.get(1));
    int count = RexLiteral.intValue(operands.get(2));
    String scoreField = operands.size() == 4 ? RexLiteral.stringValue(operands.get(3)) : null;
    return new LuceneKnnVectorFilter(field, vector, count, scoreField);
  }

  @Override
  public String toString()
  {
    return "LuceneNearestFilterConversion";
  }
}
