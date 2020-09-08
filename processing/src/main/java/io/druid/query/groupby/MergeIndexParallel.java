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

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderedLimitSpec;
import io.druid.common.guava.Comparators;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 */
public final class MergeIndexParallel extends MergeIndex.GroupByMerge
{
  private static final Logger LOG = new Logger(MergeIndexParallel.class);

  private final GroupByQuery groupBy;

  private final AggregatorFactory.Combiner[] metrics;
  private final int metricStart;

  private final Map<MergeKey, Object[]> mapping;
  private final BiFunction<MergeKey, Object[], Object[]> populator;

  public MergeIndexParallel(
      final GroupByQuery groupBy,
      final int maxRowCount,
      final int parallelism
  )
  {
    super(groupBy);
    this.groupBy = groupBy;
    this.metrics = AggregatorFactory.toCombinerArray(groupBy.getAggregatorSpecs());
    this.metricStart = groupBy.getDimensions().size() + 1;
    this.mapping = parallelism == 1 ?
                   Maps.<MergeKey, Object[]>newHashMap() :
                   new ConcurrentHashMap<MergeKey, Object[]>(16 << parallelism);

    this.populator = new BiFunction<MergeKey, Object[], Object[]>()
    {
      private final AtomicInteger counter = new AtomicInteger();

      @Override
      @SuppressWarnings("unchecked")
      public Object[] apply(final MergeKey key, final Object[] prevValue)
      {
        if (prevValue == null) {
          if (counter.incrementAndGet() >= maxRowCount) {
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

  @Override
  protected void _addRow(Object[] values)
  {
    mapping.compute(MergeKey.of(values, metricStart), populator);
  }

  @Override
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
      Comparator[] comparators = DimensionSpecs.toComparator(groupBy.getDimensions());
      Collections.sort(list, Comparators.toArrayComparator(comparators));
      return Sequences.simple(Iterables.transform(list, GroupByQueryEngine.arrayToRow(groupBy, compact)));
    }

    // sort all
    final Object[][] array = mapping.values().toArray(new Object[0][]);
    final Comparator[] comparators = DimensionSpecs.toComparator(groupBy.getDimensions(), true);
    long start = System.currentTimeMillis();
    Arrays.parallelSort(
        array, Comparators.toArrayComparator(comparators, Granularities.ALL.equals(groupBy.getGranularity()) ? 1 : 0)
    );
    LOG.debug("Took %d msec for sorting %,d rows", (System.currentTimeMillis() - start), array.length);

    return Sequences.simple(
        Iterables.transform(Arrays.asList(array), GroupByQueryEngine.arrayToRow(groupBy, compact))
    );
  }

  @Override
  public void close()
  {
    mapping.clear();
  }

  private static final Comparator comparator = GuavaUtils.nullFirstNatural();

  private static abstract class MergeKey implements Comparable<MergeKey>
  {
    static MergeKey of(final Object[] values, final int compareUpTo)
    {
      switch (compareUpTo) {
        case 1:
          return new MergeKey1(values);
        case 2:
          return new MergeKey2(values);
        case 3:
          return new MergeKey3(values);
        default:
          return new MergeKeyN(values, compareUpTo);
      }
    }

    protected MergeKey(Object[] values) {this.values = values;}

    protected final Object[] values;
  }

  private static final class MergeKeyN extends MergeKey
  {
    private final int upTo;

    private MergeKeyN(Object[] values, int upTo)
    {
      super(values);
      this.upTo = upTo;
    }

    @Override
    public final boolean equals(Object o)
    {
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
      int hash = 1;
      for (int i = 0; i < upTo; i++) {
        hash = 31 * hash + Objects.hashCode(values[i]);
      }
      return hash;
    }

    @Override
    public final String toString()
    {
      return Arrays.toString(Arrays.copyOf(values, upTo));
    }

    @Override
    @SuppressWarnings("unchecked")
    public final int compareTo(final MergeKey o)
    {
      int compare = 0;
      for (int i = 0; compare == 0 && i < upTo; i++) {
        compare = comparator.compare(values[i], o.values[i]);
      }
      return compare;
    }
  }

  private static final class MergeKey1 extends MergeKey
  {
    private MergeKey1(Object[] values) {super(values);}

    @Override
    public final boolean equals(Object o)
    {
      return Objects.equals(values[0], ((MergeKey) o).values[0]);
    }

    @Override
    public final int hashCode()
    {
      return Objects.hashCode(values[0]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(MergeKey o)
    {
      return comparator.compare(values[0], o.values[0]);
    }
  }

  private static final class MergeKey2 extends MergeKey
  {
    private MergeKey2(Object[] values) {super(values);}

    @Override
    public final boolean equals(Object o)
    {
      final MergeKey other = (MergeKey) o;
      return Objects.equals(values[0], other.values[0]) && Objects.equals(values[1], other.values[1]);
    }

    @Override
    public final int hashCode()
    {
      return 31 * Objects.hashCode(values[0]) + Objects.hashCode(values[1]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(MergeKey o)
    {
      int compare = comparator.compare(values[0], o.values[0]);
      if (compare == 0) {
        compare = comparator.compare(values[1], o.values[1]);
      }
      return compare;
    }
  }

  private static final class MergeKey3 extends MergeKey
  {
    private MergeKey3(Object[] values) {super(values);}

    @Override
    public final boolean equals(Object o)
    {
      final MergeKey other = (MergeKey) o;
      return Objects.equals(values[0], other.values[0]) &&
             Objects.equals(values[1], other.values[1]) &&
             Objects.equals(values[2], other.values[2]);
    }

    @Override
    public final int hashCode()
    {
      return 31 * 31 * Objects.hashCode(values[0]) + 31 * Objects.hashCode(values[1]) + Objects.hashCode(values[2]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(MergeKey o)
    {
      int compare = comparator.compare(values[0], o.values[0]);
      if (compare == 0) {
        compare = comparator.compare(values[1], o.values[1]);
        if (compare == 0) {
          compare = comparator.compare(values[2], o.values[2]);
        }
      }
      return compare;
    }
  }
}
