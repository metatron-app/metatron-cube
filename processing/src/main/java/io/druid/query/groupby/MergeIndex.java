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

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderedLimitSpec;
import io.druid.query.ordering.Comparators;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 */
public final class MergeIndex implements Closeable
{
  private final GroupByQuery groupBy;

  private final AggregatorFactory[] metrics;
  private final int metricStart;

  private final Map<MergeKey, Object[]> mapping;
  private final BiFunction<MergeKey, Object[], Object[]> populator;
  private final int[][] groupings;

  public MergeIndex(
      final GroupByQuery groupBy,
      final int maxRowCount,
      final int parallelism
  )
  {
    this.groupBy = groupBy;
    this.metrics = AggregatorFactory.toCombiner(groupBy.getAggregatorSpecs()).toArray(new AggregatorFactory[0]);
    this.metricStart = groupBy.getDimensions().size() + 1;
    this.groupings = groupBy.getGroupings();
    this.mapping = parallelism == 1 ?
                   Maps.<MergeKey, Object[]>newHashMap() :
                   new ConcurrentHashMap<MergeKey, Object[]>(16 << parallelism);

    this.populator = new BiFunction<MergeKey, Object[], Object[]>()
    {
      @Override
      public Object[] apply(final MergeKey key, final Object[] prevValue)
      {
        if (prevValue == null) {
          if (prevValue == null && mapping.size() >= maxRowCount) {
            throw new ISE("Maximum number of rows [%d] reached", maxRowCount);
          }
          return key.values;
        }
        for (int i = 0; i < metrics.length; i++) {
          final int index = metricStart + i;
          prevValue[index] = metrics[i].combine(prevValue[index], key.values[index]);
        }
        return prevValue;
      }
    };
  }

  public void add(Row row)
  {
    final Object[] values = ((CompactRow) row).getValues();
    if (groupings.length == 0) {
      _addRow(values);
    } else {
      for (int[] grouping : groupings) {
        Object[] copy = Arrays.copyOf(values, values.length);
        Arrays.fill(copy, 1, metricStart, null);
        for (int index : grouping) {
          copy[index + 1] = values[index + 1];
        }
        _addRow(copy);
      }
    }
  }

  private void _addRow(Object[] values)
  {
    mapping.compute(MergeKey.of(values, metricStart), populator);
  }

  public Sequence<Row> toMergeStream(final boolean compact)
  {
    final OrderedLimitSpec nodeLimit = groupBy.getLimitSpec().getNodeLimit();
    if (nodeLimit != null && nodeLimit.hasLimit() && nodeLimit.getLimit() < mapping.size()) {
      Sequence<Object[]> sequence = GroupByQueryEngine.takeTopN(groupBy, nodeLimit).apply(
          Sequences.simple(mapping.values())
      );
      if (LimitSpecs.inGroupByOrdering(groupBy, nodeLimit)) {
        return Sequences.map(sequence, GroupByQueryEngine.arrayToRow(groupBy, compact));
      }
      List<Object[]> list = Sequences.toList(sequence);
      Comparator[] comparators = DimensionSpecs.toComparatorWithDefault(groupBy.getDimensions());
      Collections.sort(list, Comparators.toArrayComparator(comparators));
      return Sequences.simple(Iterables.transform(list, GroupByQueryEngine.arrayToRow(groupBy, compact)));
    }

    // sort all
    final Comparator[] comparators = DimensionSpecs.toComparator(groupBy.getDimensions(), true);
    final Object[][] array = mapping.values().toArray(new Object[0][]);
    Arrays.parallelSort(array, new Comparator<Object[]>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public int compare(final Object[] o1, final Object[] o2)
      {
        int compare = 0;
        for (int i = 0; compare == 0 && i < comparators.length; i++) {
          compare = comparators[i].compare(o1[i], o2[i]);
        }
        return compare;
      }
    });
    return Sequences.simple(
        Iterables.transform(Arrays.asList(array), GroupByQueryEngine.arrayToRow(groupBy, compact))
    );
  }

  @Override
  public void close() throws IOException
  {
    mapping.clear();
  }

  private static final Comparator comparator = GuavaUtils.nullFirstNatural();

  private static abstract class MergeKey implements Comparable<MergeKey>
  {
    static MergeKey of(final Object[] values, final int compareUpTo)
    {
      return new MergeKey(values)
      {
        @Override
        protected int compareUpTo()
        {
          return compareUpTo;
        }
      };
    }

    private final Object[] values;

    private MergeKey(Object[] values)
    {
      this.values = values;
    }

    @Override
    public final boolean equals(Object o)
    {
      final int upTo = compareUpTo();
      final MergeKey other = (MergeKey) o;
      for (int i = 0; i < upTo; i++) {
        if (!Objects.equals(values[i], other.values[i])) {
          return false;
        }
      }
      return true;
    }

    @Override
    public final int hashCode()
    {
      final int upTo = compareUpTo();
      int hash = 1;
      for (int i = 0; i < upTo; i++) {
        hash = 31 * hash + Objects.hashCode(values[i]);
      }
      return hash;
    }

    @Override
    public final String toString()
    {
      return Arrays.toString(Arrays.copyOf(values, compareUpTo()));
    }

    protected abstract int compareUpTo();

    @Override
    @SuppressWarnings("unchecked")
    public final int compareTo(final MergeKey o)
    {
      final int upTo = compareUpTo();
      int compare = 0;
      for (int i = 0; compare == 0 && i < upTo; i++) {
        compare = comparator.compare(values[i], o.values[i]);
      }
      return compare;
    }
  }
}
