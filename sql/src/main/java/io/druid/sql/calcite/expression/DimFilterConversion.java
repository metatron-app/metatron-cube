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

import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

public interface DimFilterConversion
{
  /**
   * Name of function
   *
   * @return
   */
  String name();

  /**
   * Returns the SQL operator corresponding to this function. Should be a singleton.
   *
   * @return operator
   */
  default SqlOperator calciteOperator()
  {
    return DruidOperatorTable.dummy(name(), ReturnTypes.explicit(SqlTypeName.BOOLEAN));
  }

  /**
   * Translate a Calcite {@code RexNode} to a Druid expression.
   *
   * @param plannerContext SQL planner context
   * @param rowSignature   signature of the rows to be extracted from
   * @param rexNode        expression meant to be applied on top of the rows
   *
   * @return Druid expression, or null if translation is not possible
   *
   * @see Expressions#toDruidFilter(PlannerContext, RowSignature, RexNode)
   */
  DimFilter toDruidFilter(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode
  );
}
