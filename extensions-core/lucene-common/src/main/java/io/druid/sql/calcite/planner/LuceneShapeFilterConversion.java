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

import io.druid.common.guava.GuavaUtils;
import io.druid.query.ShapeFormat;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.LuceneShapeFilter;
import io.druid.segment.lucene.SpatialOperations;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.DimFilterConversion;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public abstract class LuceneShapeFilterConversion implements DimFilterConversion
{
  public static LuceneShapeFilterConversion of(final String name, final SpatialOperations operation)
  {
    return new LuceneShapeFilterConversion()
    {
      @Override
      public String name()
      {
        return name;
      }

      @Override
      protected SpatialOperations getOperation()
      {
        return operation;
      }

      @Override
      public String toString()
      {
        return String.format("LuceneShapeFilterConversion[%s]", operation);
      }
    };
  }

  @Override
  public DimFilter toDruidFilter(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    List<RexNode> operands = Utils.operands(rexNode);
    if (operands.size() != 2 && operands.size() != 3 && operands.size() != 4) {
      return null;
    }
    String field = DruidOperatorTable.getFieldName(operands.get(0), plannerContext, rowSignature);
    ShapeFormat format = null;
    String scoreField = null;
    if (operands.size() >= 4) {
      String param1 = RexLiteral.stringValue(operands.get(1));
      ShapeFormat check = ShapeFormat.check(param1);
      if (check != null) {
        format = check;
        if (operands.size() == 4) {
          scoreField = RexLiteral.stringValue(operands.get(2));
        }
      } else {
        scoreField = param1;
        if (operands.size() == 4) {
          format = ShapeFormat.fromString(RexLiteral.stringValue(operands.get(2)));
        }
      }
    }
    String shape = RexLiteral.stringValue(GuavaUtils.lastOf(operands));
    return new LuceneShapeFilter(field, getOperation(), format, shape, scoreField);
  }

  protected abstract SpatialOperations getOperation();
}
