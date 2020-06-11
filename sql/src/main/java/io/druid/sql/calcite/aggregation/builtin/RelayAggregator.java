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
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
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

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexBuilder rexBuilder,
      String name,
      AggregateCall aggregateCall,
      Project project,
      List<Aggregation> existingAggregations,
      boolean finalizeAggregations
  )
  {
    final List<Integer> arguments = aggregateCall.getArgList();
    if (arguments.size() != 1) {
      return null; // todo
    }
    final RexNode rexNode = Expressions.fromFieldAccess(
        rowSignature,
        project,
        Iterables.getOnlyElement(aggregateCall.getArgList())
    );

    final DruidExpression arg = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
    if (arg == null) {
      return null;
    }

    final String fieldName;
    final List<VirtualColumn> virtualColumns = new ArrayList<>();
    final String aggregatorName = finalizeAggregations ? Calcites.makePrefixedName(name, "a") : name;

    final ValueDesc valueDesc = Calcites.getValueDescForRelDataType(rexNode.getType());
    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
    } else {
      ExprVirtualColumn virtualColumn = arg.toVirtualColumn(Calcites.makePrefixedName(name, "v"));
      virtualColumns.add(virtualColumn);
      fieldName = virtualColumn.getOutputName();
    }
    final AggregatorFactory factory = new RelayAggregatorFactory(
        aggregatorName,
        fieldName,
        valueDesc.typeName(),
        relayType.name()
    );

    return Aggregation.create(
        rowSignature,
        virtualColumns,
        Collections.singletonList(factory),
        finalizeAggregations ? AggregatorFactory.asFinalizer(name, factory) : null
    );
  }
}
