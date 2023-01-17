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
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.planner.Calcites;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;

public class AvgSqlAggregator implements SqlAggregator
{
  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.AVG;
  }

  @Override
  public boolean register(Aggregations aggregations, DimFilter predicate, AggregateCall call, String outputName)
  {
    DruidExpression expression = aggregations.getNonDistinctSingleArgument(call);
    if (expression == null) {
      return false;
    }
    String inputName = aggregations.registerColumn(outputName, expression);
    ValueDesc outputType = toSumType(call.type);

    String sumName = Calcites.makePrefixedName(outputName, "sum");
    aggregations.register(GenericSumAggregatorFactory.of(sumName, inputName, outputType), predicate);

    String countName = Calcites.makePrefixedName(outputName, "count");
    aggregations.register(CountAggregatorFactory.of(countName, inputName), predicate);

    aggregations.register(
        new ArithmeticPostAggregator(
            outputName, "quotient",
            Arrays.asList(FieldAccessPostAggregator.of(sumName), FieldAccessPostAggregator.of(countName))
        )
    );
    return true;
  }

  private static ValueDesc toSumType(RelDataType type)
  {
    // Use 64-bit sum regardless of the type of the AVG aggregator.
    SqlTypeName typeName = type.getSqlTypeName();
    if (SqlTypeName.INT_TYPES.contains(typeName)) {
      return ValueDesc.LONG;
    } else if (SqlTypeName.DECIMAL.equals(typeName)) {
      return ValueDesc.DECIMAL;
    }
    return ValueDesc.DOUBLE;
  }
}
