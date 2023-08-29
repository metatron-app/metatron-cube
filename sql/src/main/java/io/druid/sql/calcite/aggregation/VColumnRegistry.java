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

package io.druid.sql.calcite.aggregation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.Pair;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactory.FieldExpressionSupport;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.planner.Calcites;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VColumnRegistry
{
  private final Map<DruidExpression, ReferenceCounter<VirtualColumn>> virtualColumns = Maps.newLinkedHashMap();

  private static class ReferenceCounter<T>
  {
    private final T object;
    private int counter;

    private ReferenceCounter(T object) {this.object = object;}
  }

  public VirtualColumn register(String outputName, DruidExpression expression)
  {
    ReferenceCounter<VirtualColumn> vc = virtualColumns.computeIfAbsent(
        expression, k -> new ReferenceCounter<>(k.toVirtualColumn(Calcites.vcName(outputName)))
    );
    vc.counter++;
    return vc.object;
  }

  public List<AggregatorFactory> simplify(Iterable<AggregatorFactory> aggregators)
  {
    Map<String, Pair<DruidExpression, String>> mapping = Maps.newLinkedHashMap();
    for (Map.Entry<DruidExpression, ReferenceCounter<VirtualColumn>> entry : virtualColumns.entrySet()) {
      ReferenceCounter<VirtualColumn> h = entry.getValue();
      if (h.counter == 1 && h.object instanceof ExprVirtualColumn) {
        mapping.put(h.object.getOutputName(), Pair.of(entry.getKey(), ((ExprVirtualColumn) h.object).getExpression()));
      }
    }
    if (mapping.isEmpty() || !Iterables.any(aggregators, f -> f instanceof FieldExpressionSupport)) {
      return Lists.newArrayList(aggregators);
    }
    List<AggregatorFactory> simplified = Lists.newArrayList();
    for (AggregatorFactory factory : aggregators) {
      if (factory instanceof FieldExpressionSupport) {
        FieldExpressionSupport exprSupport = ((FieldExpressionSupport) factory);
        Pair<DruidExpression, String> pair = mapping.remove(exprSupport.getFieldName());
        if (pair != null) {
          virtualColumns.remove(pair.lhs);
          factory = exprSupport.withFieldExpression(pair.rhs);
        }
      }
      simplified.add(factory);
    }
    return simplified;
  }

  public List<VirtualColumn> virtualColumns()
  {
    return virtualColumns.values().stream().map(h -> h.object).collect(Collectors.toList());
  }
}
