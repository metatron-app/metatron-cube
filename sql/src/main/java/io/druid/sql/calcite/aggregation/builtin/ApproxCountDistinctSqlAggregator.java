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

import com.google.common.collect.Iterables;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.planner.Calcites;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;

import java.util.List;

public class ApproxCountDistinctSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new ApproxCountDistinctSqlAggFunction();
  private static final String NAME = "APPROX_COUNT_DISTINCT";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Override
  public boolean register(Aggregations aggregations, DimFilter predicate, AggregateCall call, String outputName)
  {
    final List<DruidExpression> expressions = aggregations.argumentsToExpressions(call.getArgList());
    if (GuavaUtils.isNullOrEmpty(expressions)) {
      return false;
    }

    final String aggregatorName = aggregations.isFinalizing() ? Calcites.makePrefixedName(outputName, "a") : outputName;

    final DruidExpression first = expressions.get(0);
    final AggregatorFactory factory;
    if (expressions.size() == 1 && first.isDirectColumnAccess() &&
        HyperLogLogCollector.HLL_TYPE.equals(aggregations.resolve(first.getDirectColumn()))) {
      factory = HyperUniquesAggregatorFactory.of(aggregatorName, first.getDirectColumn());
    } else if (Iterables.all(expressions, DruidExpression::isDirectColumnAccess)) {
      List<String> fields = GuavaUtils.transform(expressions, DruidExpression::getDirectColumn);
      factory = CardinalityAggregatorFactory.fields(aggregatorName, fields, GroupingSetSpec.EMPTY);
    } else {
      List<DimensionSpec> dimensions = GuavaUtils.transform(
          expressions, expression -> aggregations.registerDimension(aggregatorName, expression)
      );
      factory = CardinalityAggregatorFactory.dimensions(aggregatorName, dimensions, GroupingSetSpec.EMPTY);
    }
    aggregations.register(factory, predicate);

    if (aggregations.isFinalizing()) {
      aggregations.register(new HyperUniqueFinalizingPostAggregator(outputName, factory.getName(), true));
    }
    return true;
  }

  private static class ApproxCountDistinctSqlAggFunction extends SqlAggFunction
  {
    private ApproxCountDistinctSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          InferTypes.VARCHAR_1024,
          OperandTypes.ANY,
          SqlFunctionCategory.STRING,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
