/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby;

import com.metamx.common.guava.nary.BinaryFn;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.List;

public class GroupByBinaryFnV2 implements BinaryFn<Row, Row, Row>
{
  private final AggregatorFactory[] aggregatorFactories;

  public GroupByBinaryFnV2(GroupByQuery query)
  {
    List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
    this.aggregatorFactories = new AggregatorFactory[aggregatorSpecs.size()];
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregatorFactories[i] = aggregatorSpecs.get(i);
    }
  }

  @Override
  public Row apply(final Row arg1, final Row arg2)
  {
    if (arg1 == null) {
      return arg2;
    } else if (arg2 == null) {
      return arg1;
    }

    Row.Updatable updatable = Rows.toUpdatable(arg1);
    for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
      final String name = aggregatorFactory.getName();
      updatable.set(name, aggregatorFactory.combine(updatable.getRaw(name), arg2.getRaw(name)));
    }

    return updatable;
  }
}
