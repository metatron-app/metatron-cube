/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.query.groupby;

import io.druid.collections.IntList;
import io.druid.common.guava.CombineFn;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Row;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.Arrays;

@SuppressWarnings("unchecked")
public class AggregationCombineFn implements CombineFn.Identical<Row>
{
  public static AggregationCombineFn of(BaseAggregationQuery query, boolean finalize)
  {
    CombineFn[] combiners = AggregatorFactory.toCombiner(query.getAggregatorSpecs(), finalize);
    int[] finalizing = IntList.collect(combiners, c -> c instanceof CombineFn.Finalizing).array();
    if (finalizing.length == 0) {
      return new AggregationCombineFn(query.getDimensions().size() + 1, combiners);
    }
    return new AggregationCombineFn(query.getDimensions().size() + 1, combiners)
    {
      @Override
      public Row done(Row arg)
      {
        final Object[] values = ((CompactRow) arg).getValues();
        for (int ix : finalizing) {
          values[start + ix] = combiners[ix].done(values[start + ix]);
        }
        return arg;
      }
    };
  }

  final int start;
  final CombineFn[] combiners;

  private AggregationCombineFn(int start, CombineFn[] combiners)
  {
    this.start = start;
    this.combiners = combiners;
  }

  @Override
  public Row apply(final Row arg1, final Row arg2)
  {
    if (arg1 == null) {
      return arg2;
    } else if (arg2 == null) {
      return arg1;
    }
    final Object[] values1 = ((CompactRow) arg1).getValues();
    final Object[] values2 = ((CompactRow) arg2).getValues();
    int index = start;
    for (CombineFn combiner : combiners) {
      values1[index] = combiner.apply(values1[index], values2[index]);
      index++;
    }
    Arrays.fill(values2, null);   // for faster gc
    return arg1;
  }
}
