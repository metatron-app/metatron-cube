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
import io.druid.query.aggregation.Aggregators;
import io.druid.segment.Capabilities;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * This has to be its own strategy because the pooled topn algorithm assumes each index is unique, and cannot handle multiple index numerals referencing the same dimension value.
 */
public class DimExtractionTopNAlgorithm extends BaseTopNAlgorithm<Object[][], Map<Object, Object[]>, TopNParams>
{
  private final TopNQuery query;

  public DimExtractionTopNAlgorithm(Capabilities capabilities, TopNQuery query)
  {
    super(capabilities);
    this.query = query;
  }

  @Override
  public TopNParams makeInitParams(
      final DimensionSelector dimSelector,
      final List<AggregatorFactory> aggregators,
      final Cursor cursor
  )
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
  protected Object[][] makeDimValSelector(TopNParams params, int numProcessed, int numToProcess)
  {
    final ObjectArrayProvider provider = new ObjectArrayProvider(
        params.getDimSelector(),
        query,
        params.getCardinality()
    );

    // Unlike regular topN we cannot rely on ordering to optimize.
    // Optimization possibly requires a reverse lookup from value to ID, which is
    // not possible when applying an extraction function
    return provider.build();
  }

  @Override
  protected Object[][] updateDimValSelector(Object[][] aggregators, int numProcessed, int numToProcess)
  {
    return aggregators;
  }

  @Override
  protected Map<Object, Object[]> makeDimValAggregateStore(TopNParams params)
  {
    return Maps.newHashMap();
  }

  @Override
  public void scanAndAggregate(
      TopNParams params,
      Object[][] rowSelector,
      Map<Object, Object[]> aggregatesStore,
      int numProcessed
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
      final IndexedInts dimValues = dimSelector.getRow();

      for (int i = 0; i < dimValues.size(); ++i) {
        final int dimIndex = dimValues.get(i);
        Object[] values = rowSelector[dimIndex];
        if (values == null) {
          values = aggregatesStore.computeIfAbsent(dimSelector.lookupName(dimIndex), populator);
          rowSelector[dimIndex] = values;
        }
        Aggregators.aggregate(values, aggregators);
      }
      cursor.advance();
    }
    Aggregators.close(aggregators);
  }

  @Override
  protected void updateResults(
      TopNParams params,
      Object[][] rowSelector,
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
