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

package io.druid.query.aggregation.bloomfilter;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.guava.DSuppliers;
import io.druid.query.RowResolver;
import io.druid.query.aggregation.HashAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;

import java.util.List;

public class BloomFilterAggregator extends HashAggregator.ScanSupport<BloomKFilter>
{
  public static BloomKFilter build(
      RowResolver resolver,
      List<String> columns,
      long maxNumEntries,
      Iterable<Object[]> values
  ) {
    final DSuppliers.HandOver<Object[]> handover = new DSuppliers.HandOver<Object[]>();
    final ColumnSelectorFactory factory = new ColumnSelectorFactories.FromArraySupplier(handover, resolver);
    final List<DimensionSelector> selectors = Lists.newArrayList(
        Iterables.transform(columns, column -> factory.makeDimensionSelector(DefaultDimensionSpec.of(column)))
    );
    final BloomKFilter bloom = new BloomKFilter(maxNumEntries);
    final BloomFilterAggregator aggregator = new BloomFilterAggregator(
        ValueMatcher.TRUE, selectors, GroupingSetSpec.EMPTY_INDEX, true, maxNumEntries
    );
    for (Object[] value : values) {
      handover.set(value);
      aggregator.aggregate(bloom);
    }
    return bloom;
  }

  private final long maxNumEntries;

  public BloomFilterAggregator(
      ValueMatcher predicate,
      List<DimensionSelector> selectorList,
      int[][] groupings,
      boolean byRow,
      long maxNumEntries
  )
  {
    super(predicate, selectorList, groupings, byRow);
    this.maxNumEntries = maxNumEntries;
  }

  @Override
  protected final BloomKFilter newCollector()
  {
    return new BloomKFilter(maxNumEntries);
  }
}
