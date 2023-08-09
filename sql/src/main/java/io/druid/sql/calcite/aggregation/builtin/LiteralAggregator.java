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

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactory.LiteralAggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.planner.Calcites;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;

public class LiteralAggregator implements SqlAggregator
{
  private final SqlAggFunction function;

  public LiteralAggregator(SqlAggFunction function)
  {
    this.function = function;
  }

  @Override
  public SqlAggFunction calciteFunction()
  {
    return function;
  }

  @Override
  public boolean register(Aggregations aggregations, DimFilter predicate, AggregateCall call, String outputName)
  {
    RexNode rexNode = call.rexList.get(0);
    Object literal = Utils.extractLiteral((RexLiteral) rexNode, aggregations.context());
    AggregatorFactory factory = new LiteralAggregatorFactory(outputName, Calcites.asValueDesc(call.type), literal);
    aggregations.register(factory, predicate);
    return true;
  }
}
