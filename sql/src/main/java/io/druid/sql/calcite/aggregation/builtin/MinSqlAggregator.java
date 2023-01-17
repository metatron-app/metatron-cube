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

package io.druid.sql.calcite.aggregation.builtin;

import io.druid.data.ValueDesc;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.planner.Calcites;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class MinSqlAggregator implements SqlAggregator
{
  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.MIN;
  }

  @Override
  public boolean register(Aggregations aggregations, DimFilter predicate, AggregateCall call, String outputName)
  {
    DruidExpression expression = aggregations.getNonDistinctSingleArgument(call);
    if (expression == null) {
      return false;
    }
    String inputName = aggregations.registerColumn(outputName, expression);
    ValueDesc outputType = Calcites.asValueDesc(call.getType());

    AggregatorFactory factory;
    if (outputType.isNumeric() || outputType.isArray()) {
      factory = GenericMinAggregatorFactory.of(outputName, inputName, outputType);
    } else if (expression.isDirectColumnAccess()) {
      factory = RelayAggregatorFactory.min(outputName, expression.getDirectColumn(), outputType.typeName());
    } else {
      throw new UnsupportedOperationException("Not supports min on non-numeric expression");
    }
    aggregations.register(factory, predicate);
    return true;
  }
}
