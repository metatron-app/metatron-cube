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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderedLimitSpec;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
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

  private final Function<Object[], MergeKey> factory;

  private final Map<MergeKey, Object[]> mapping;
  private final BiFunction<MergeKey, Object[], Object[]> populator;

  public MergeIndexParallel(
      final GroupByQuery groupBy,
      final int maxRowCount,
      final int parallelism
  )
  {
    super(groupBy);
    this.factory = MergeKey.of(groupBy.getGranularity(), metricStart);
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
          prevValue[index] = metrics[i].apply(prevValue[index], key.values[index]);
        }
        return prevValue;
      }
    };
  }

  @Override
  protected void _addRow(Object[] values)
  {
    mapping.compute(factory.apply(values), populator);
  }

  @Override
  public Sequence<Row> toMergeStream(boolean parallel, boolean compact)
  {
    Collection<Object[]> values = mapping.values();
    final OrderedLimitSpec nodeLimit = groupBy.getLimitSpec().getNodeLimit();
    if (nodeLimit != null && nodeLimit.getLimit() > 0) {
      Sequence<Object[]> sequence = GroupByQueryEngine.takeTopN(groupBy, nodeLimit, values);
      if (LimitSpecs.inGroupByOrdering(groupBy, nodeLimit)) {
        return Sequences.map(sequence, GroupByQueryEngine.arrayToRow(groupBy, compact));
      }
      values = Sequences.toList(sequence);
    }

    // sort all
    final Comparator<Object[]> cmp = DimensionSpecs.toComparator(groupBy.getDimensions(), groupBy.getGranularity());
    final long start = System.currentTimeMillis();
    final Object[][] array = sortRows(values, cmp, parallel);
    LOG.debug("Took %d msec for sorting %,d rows", (System.currentTimeMillis() - start), array.length);

    return Sequences.simple(
        groupBy.estimatedInitialColumns(),
        Iterables.transform(Arrays.asList(array), GroupByQueryEngine.arrayToRow(groupBy, compact))
    );
  }

  private static Object[][] sortRows(Collection<Object[]> values, Comparator<Object[]> cmp, boolean parallel)
  {
    final Object[][] array = values.toArray(new Object[0][]);
    if (parallel) {
      Arrays.parallelSort(array, cmp);
    } else {
      Arrays.sort(array, cmp);
    }
    return array;
  }

  @Override
  public void close()
  {
    mapping.clear();
  }

  private static final Comparator comparator = GuavaUtils.nullFirstNatural();

  private static abstract class MergeKey implements Comparable<MergeKey>
  {
    private static Function<Object[], MergeKey> of(final Granularity granularity, final int compareUpTo)
    {
      final boolean ignoreTime = Granularities.isAll(granularity);
      switch (compareUpTo) {
        case 1:
          return ignoreTime ? v -> new MergeKey11(v) : v -> new MergeKey01(v);
        case 2:
          return ignoreTime ? v -> new MergeKey12(v) : v -> new MergeKey02(v);
        case 3:
          return ignoreTime ? v -> new MergeKey13(v) : v -> new MergeKey03(v);
        case 4:
          return ignoreTime ? v -> new MergeKey14(v) : v -> new MergeKey04(v);
        default:
          return v -> new MergeKeyN(v)
          {
            @Override
            protected int upTo() { return compareUpTo;}
          };
      }
    }

    protected MergeKey(Object[] values) {this.values = values;}

    protected final Object[] values;
  }

  private static abstract class MergeKeyN extends MergeKey
  {
    protected abstract int upTo();

    private MergeKeyN(Object[] values)
    {
      super(values);
    }

    @Override
    public final boolean equals(Object o)
    {
      final int upTo = upTo();
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
      final int upTo = upTo();
      int hash = 1;
      for (int i = 0; i < upTo; i++) {
        hash = 31 * hash + Objects.hashCode(values[i]);
      }
      return hash;
    }

    @Override
    public final String toString()
    {
      return Arrays.toString(Arrays.copyOf(values, upTo()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public final int compareTo(final MergeKey o)
    {
      final int upTo = upTo();
      int compare = 0;
      for (int i = 0; compare == 0 && i < upTo; i++) {
        compare = comparator.compare(values[i], o.values[i]);
      }
      return compare;
    }
  }

  private static final class MergeKey01 extends MergeKey
  {
    private MergeKey01(Object[] values) {super(values);}

    @Override
    public boolean equals(Object o)
    {
      return Objects.equals(values[0], ((MergeKey) o).values[0]);
    }

    @Override
    public int hashCode()
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

  private static final class MergeKey11 extends MergeKey
  {
    private MergeKey11(Object[] values) {super(values);}

    @Override
    public boolean equals(Object o)
    {
      return true;
    }

    @Override
    public int hashCode()
    {
      return 0;
    }

    @Override
    public int compareTo(MergeKey o)
    {
      return 0;
    }
  }

  private static final class MergeKey02 extends MergeKey
  {
    private MergeKey02(Object[] values) {super(values);}

    @Override
    public boolean equals(Object o)
    {
      final MergeKey other = (MergeKey) o;
      return Objects.equals(values[0], other.values[0]) && Objects.equals(values[1], other.values[1]);
    }

    @Override
    public int hashCode()
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

  private static final class MergeKey12 extends MergeKey
  {
    private MergeKey12(Object[] values) {super(values);}

    @Override
    public boolean equals(Object o)
    {
      final MergeKey other = (MergeKey) o;
      return Objects.equals(values[1], other.values[1]);
    }

    @Override
    public int hashCode()
    {
      return 31 * Objects.hashCode(values[1]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(MergeKey o)
    {
      return comparator.compare(values[1], o.values[1]);
    }
  }

  private static final class MergeKey03 extends MergeKey
  {
    private MergeKey03(Object[] values) {super(values);}

    @Override
    public boolean equals(Object o)
    {
      final MergeKey other = (MergeKey) o;
      return Objects.equals(values[0], other.values[0]) &&
             Objects.equals(values[1], other.values[1]) &&
             Objects.equals(values[2], other.values[2]);
    }

    @Override
    public int hashCode()
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

  private static final class MergeKey13 extends MergeKey
  {
    private MergeKey13(Object[] values) {super(values);}

    @Override
    public boolean equals(Object o)
    {
      final MergeKey other = (MergeKey) o;
      return Objects.equals(values[1], other.values[1]) &&
             Objects.equals(values[2], other.values[2]);
    }

    @Override
    public int hashCode()
    {
      return 31 * Objects.hashCode(values[1]) + Objects.hashCode(values[2]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(MergeKey o)
    {
      int compare = comparator.compare(values[1], o.values[1]);
      if (compare == 0) {
        compare = comparator.compare(values[2], o.values[2]);
      }
      return compare;
    }
  }

  private static final class MergeKey04 extends MergeKey
  {
    private MergeKey04(Object[] values) {super(values);}

    @Override
    public boolean equals(Object o)
    {
      final MergeKey other = (MergeKey) o;
      return Objects.equals(values[0], other.values[0]) &&
             Objects.equals(values[1], other.values[1]) &&
             Objects.equals(values[2], other.values[2]) &&
             Objects.equals(values[3], other.values[3]);
    }

    @Override
    public int hashCode()
    {
      return 31 * 31 * 31 * Objects.hashCode(values[0]) +
             31 * 31 * Objects.hashCode(values[1]) +
             31 * Objects.hashCode(values[2]) +
             Objects.hashCode(values[3]);
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
          if (compare == 0) {
            compare = comparator.compare(values[3], o.values[3]);
          }
        }
      }
      return compare;
    }
  }

  private static final class MergeKey14 extends MergeKey
  {
    private MergeKey14(Object[] values) {super(values);}

    @Override
    public boolean equals(Object o)
    {
      final MergeKey other = (MergeKey) o;
      return Objects.equals(values[1], other.values[1]) &&
             Objects.equals(values[2], other.values[2]) &&
             Objects.equals(values[3], other.values[3]);
    }

    @Override
    public int hashCode()
    {
      return 31 * 31 * Objects.hashCode(values[1]) + 31 * Objects.hashCode(values[2]) + Objects.hashCode(values[3]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(MergeKey o)
    {
      int compare = comparator.compare(values[1], o.values[1]);
      if (compare == 0) {
        compare = comparator.compare(values[2], o.values[2]);
        if (compare == 0) {
          compare = comparator.compare(values[3], o.values[3]);
        }
      }
      return compare;
    }
  }
}
