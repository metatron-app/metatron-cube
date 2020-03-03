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

package io.druid.sql.calcite.rule;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import io.druid.sql.calcite.rel.DruidValuesRel;
import io.druid.sql.calcite.rel.QueryMaker;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;
import java.util.TimeZone;
import java.util.function.Predicate;

public class DruidValuesRule extends ConverterRule
{
  private final QueryMaker maker;

  public DruidValuesRule(QueryMaker maker)
  {
    super(LogicalValues.class, (Predicate<RelNode>) r -> true,
          Convention.NONE, BindableConvention.INSTANCE, RelFactories.LOGICAL_BUILDER,
          "BindableValuesRule"
    );
    this.maker = maker;
  }

  @Override
  @SuppressWarnings("unchecked")
  public RelNode convert(RelNode rel)
  {
    Iterable<Object[]> converted = Iterables.transform(
        ((LogicalValues) rel).tuples,
        new Function<List<RexLiteral>, Object[]>()
        {
          private final TimeZone timeZone = TimeZone.getTimeZone(maker.getPlannerContext().getTimeZone().getID());

          @Override
          public Object[] apply(List<RexLiteral> input)
          {
            final Object[] array = new Object[input.size()];
            for (int i = 0; i < array.length; i++) {
              array[i] = input.get(i).getValue();
            }
            return array;
          }
        }
    );
    return new DruidValuesRel(rel.getCluster(), rel.getTraitSet(), rel, converted, maker);
  }
}