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

import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import javax.annotation.Nullable;
import java.util.List;

public class CountSqlAggregator implements SqlAggregator
{
  private static final ApproxCountDistinctSqlAggregator APPROX_COUNT_DISTINCT = new ApproxCountDistinctSqlAggregator();

  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlStdOperatorTable.COUNT;
  }

  @Nullable
  @Override
  public boolean register(Aggregations aggregations, DimFilter predicate, AggregateCall call, String outputName)
  {
    final List<Integer> fields = call.getArgList();
    if (fields.isEmpty()) {
      // COUNT(*)
      aggregations.register(CountAggregatorFactory.of(outputName), predicate);
      return true;
    } else if (call.isDistinct()) {
      // COUNT(DISTINCT x)
      if (aggregations.useApproximateCountDistinct()) {
        return APPROX_COUNT_DISTINCT.register(aggregations, predicate, call, outputName);
      }
    } else if (fields.size() == 1) {
      // Not COUNT(*), not distinct
      // COUNT(x) should count all non-null values of x.
      final RexNode rexNode = aggregations.toFieldRef(fields.get(0));
      if (rexNode.getType().isNullable()) {
        final DruidExpression expression = aggregations.toExpression(rexNode);
        if (expression.isDirectColumnAccess()) {
          // it's fieldName.. fieldExpression later if really needed
          aggregations.register(CountAggregatorFactory.of(outputName, expression.getDirectColumn()), predicate);
        } else {
          DimFilter filter = aggregations.toFilter(aggregations.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexNode));
          if (filter == null) {
            // Don't expect this to happen.
            throw new ISE("Could not create not-null filter for rexNode[%s]", rexNode);
          }
          String column = aggregations.registerColumn(outputName, expression);
          aggregations.register(CountAggregatorFactory.of(outputName, column), DimFilters.and(predicate, filter));
        }
      } else {
        aggregations.register(CountAggregatorFactory.of(outputName), predicate);
      }
      return true;
    }
    return false;
  }
}
