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
package io.druid.query;

import io.druid.common.guava.CombineFn;
import io.druid.common.guava.DSuppliers;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Row;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.Aggregators;
import io.druid.segment.ColumnSelectorFactories;

@SuppressWarnings("unchecked")
public class StreamAggregationFn implements CombineFn.Identical<Row>
{
  public static StreamAggregationFn of(BaseAggregationQuery query, RowResolver resolver)
  {
    return new StreamAggregationFn(query, resolver);
  }

  private final DSuppliers.HandOver<Object[]> handover = new DSuppliers.HandOver<>();
  private final Aggregator[] aggregators;
  private final int ix;

  private StreamAggregationFn(BaseAggregationQuery query, RowResolver resolver)
  {
    this.aggregators = Aggregators.makeAggregators(
        query.getAggregatorSpecs(), new ColumnSelectorFactories.FromArraySupplier(handover, resolver), true
    );
    this.ix = query.getDimensions().size() + 1;
  }

  @Override
  public Row apply(Row arg1, Row arg2)
  {
    final Object[] values;
    final Object[] values2 = ((CompactRow) arg2).getValues();
    if (arg1 == null) {
      values = new Object[ix + aggregators.length];
      System.arraycopy(values2, 0, values, 0, ix); // copy time and dimensions
      arg1 = new CompactRow(values);
    } else {
      values = ((CompactRow) arg1).getValues();
    }
    handover.set(values2);
    aggregate(values);
    return arg1;
  }

  private void aggregate(Object[] values)
  {
    int index = ix;
    for (Aggregator aggregator : aggregators) {
      values[index] = aggregator.aggregate(values[index]);
      index++;
    }
  }

  @Override
  public Row done(Row row)
  {
    final Object[] values = ((CompactRow) row).getValues();
    int index = ix;
    for (Aggregator aggregator : aggregators) {
      values[index] = aggregator.get(values[index]);
      index++;
    }
    return row;
  }
}
