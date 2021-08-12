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

package io.druid.query.topn;

import com.google.common.collect.Maps;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.Capabilities;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TimeExtractionTopNAlgorithm extends BaseTopNAlgorithm<int[], Map<Object, Object[]>, TopNParams>
{
  public static final int[] EMPTY_INTS = new int[]{};
  private final TopNQuery query;

  public TimeExtractionTopNAlgorithm(Capabilities capabilities, TopNQuery query)
  {
    super(capabilities);
    this.query = query;
  }


  @Override
  public TopNParams makeInitParams(DimensionSelector dimSelector, List<AggregatorFactory> aggregators, Cursor cursor)
  {
    return new TopNParams(
        dimSelector,
        aggregators,
        cursor,
        dimSelector.getValueCardinality(),
        Integer.MAX_VALUE
    );
  }

  @Override
  protected int[] makeDimValSelector(TopNParams params, int numProcessed, int numToProcess)
  {
    return EMPTY_INTS;
  }

  @Override
  protected int[] updateDimValSelector(int[] dimValSelector, int numProcessed, int numToProcess)
  {
    return dimValSelector;
  }

  @Override
  protected Map<Object, Object[]> makeDimValAggregateStore(TopNParams params)
  {
    return Maps.newHashMap();
  }

  @Override
  protected void scanAndAggregate(
      TopNParams params, int[] dimValSelector, Map<Object, Object[]> aggregatesStore, int numProcessed
  )
  {
    final Cursor cursor = params.getCursor();
    final DimensionSelector dimSelector = params.getDimSelector();
    final ColumnSelectorFactory factory = params.getFactory();

    final Aggregator[] aggregators = params.getAggregators();
    final Function<Object, Object[]> populator = new Function<Object, Object[]>()
    {
      @Override
      public Object[] apply(Object comparable)
      {
        return new Object[aggregators.length];
      }
    };
    while (!cursor.isDone()) {
      final Object key = dimSelector.lookupName(dimSelector.getRow().get(0));
      final Object[] values = aggregatesStore.computeIfAbsent(key, populator);
      for (int i = 0; i < aggregators.length; i++) {
        values[i] = aggregators[i].aggregate(values[i]);
      }

      cursor.advance();
    }
  }

  @Override
  protected void updateResults(
      TopNParams params,
      int[] dimValSelector,
      Map<Object, Object[]> aggregatesStore,
      TopNResultBuilder resultBuilder
  )
  {
    final Aggregator[] aggregators = params.getAggregators();
    for (Map.Entry<Object, Object[]> entry : aggregatesStore.entrySet()) {
      Object[] aggs = entry.getValue();
      if (aggs != null && aggs.length > 0) {
        final Object[] vals = new Object[aggs.length];
        for (int i = 0; i < aggs.length; i++) {
          vals[i] = aggregators[i].get(aggs[i]);
        }

        resultBuilder.addEntry(
            entry.getKey(),
            entry.getKey(),
            vals
        );
      }
    }
  }

  @Override
  protected void closeAggregators(TopNParams params, Map<Object, Object[]> stringMap)
  {
    super.closeAggregators(params, stringMap);
    stringMap.clear();
  }

  @Override
  public void cleanup(TopNParams params)
  {

  }
}
