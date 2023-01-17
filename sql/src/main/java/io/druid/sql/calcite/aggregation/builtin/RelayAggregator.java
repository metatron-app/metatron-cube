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
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.planner.Calcites;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;

import java.util.List;

public class RelayAggregator implements SqlAggregator
{
  private final Aggregators.RELAY_TYPE relayType;
  private final SqlAggFunction function;

  public RelayAggregator(SqlAggFunction function, Aggregators.RELAY_TYPE relayType)
  {
    this.function = function;
    this.relayType = relayType;
  }

  @Override
  public SqlAggFunction calciteFunction()
  {
    return function;
  }

  @Override
  public boolean register(Aggregations aggregations, DimFilter predicate, AggregateCall call, String outputName)
  {
    List<Integer> arguments = call.getArgList();
    if (arguments.size() != 1) {
      return false; // todo
    }
    RexNode rexNode = aggregations.toRexNode(arguments.get(0));
    DruidExpression arg = aggregations.toExpression(rexNode);
    if (arg == null) {
      return false;
    }

    String aggregatorName = aggregations.isFinalizing() ? Calcites.makePrefixedName(outputName, "a") : outputName;
    String inputName = aggregations.registerColumn(outputName, arg);
    ValueDesc valueDesc = Calcites.asValueDesc(rexNode.getType());
    AggregatorFactory factory = new RelayAggregatorFactory(
        aggregatorName, inputName, valueDesc.typeName(), relayType.name()
    );
    aggregations.register(factory, predicate);
    if (aggregations.isFinalizing()) {
      aggregations.register(AggregatorFactory.asFinalizer(outputName, factory));
    }
    return true;
  }
}
