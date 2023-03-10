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

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.Aggregators;
import io.druid.segment.Cursor;
import org.apache.commons.lang.mutable.MutableLong;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public final class AggregateIndex
{
  public static AggregateIndex of(int[] indices, int[] mvs)
  {
    return new AggregateIndex(indices, Arrays.stream(indices).filter(x -> x >= 0).toArray(), mvs);
  }

  private final int[] dimensions;
  private final int[] indices;
  private final int[] mvs;
  private final Map<MergeKey, Object[]> mapping;

  private AggregateIndex(int[] dimensions, int[] indices, int[] mvs)
  {
    this.dimensions = dimensions;
    this.indices = indices;
    this.mvs = mvs;
    this.mapping = Maps.<MergeKey, Object[]>newHashMap();
  }

  @SuppressWarnings("unchecked")
  public Sequence<Object[]> aggregate(GroupByQuery groupBy, Sequence<Cursor> cursors)
  {
    return Sequences.concat(Sequences.map(cursors, cursor -> {
      final Aggregator[] metrics = Aggregators.makeAggregators(groupBy.getAggregatorSpecs(), cursor);
      final BiFunction<MergeKey, Object[], Object[]> populator = Aggregators.populator(metrics);
      final int[][] groupings = groupBy.getGroupings();
      final Consumer<Object[]> consumer;
      if (groupings.length == 0) {
        consumer = values -> populate(values, populator);
      } else {
        final int metricStart = groupBy.getDimensions().size() + 1;
        consumer = values -> {
          for (final int[] grouping : groupings) {
            final Object[] copy = Arrays.copyOf(values, values.length);
            Arrays.fill(copy, 1, metricStart, null);
            for (int index : grouping) {
              copy[index + 1] = values[index + 1];
            }
            populate(values, populator);
          }
        };
      }
      final Supplier<Object[]> supplier = (Supplier<Object[]>) cursor;
      for (; !cursor.isDone(); cursor.advance()) {
        consumer.accept(supplier.get());
      }
      final MutableLong timestamp = new MutableLong(cursor.getStartTime());
      final int length = 1 + dimensions.length + metrics.length;
      return Sequences.simple(Iterables.transform(mapping.entrySet(), e -> {
        final Object[] target = new Object[length];
        final Object[] source = e.getKey().values;
        final Object[] values = e.getValue();
        target[0] = timestamp;
        for (int i = 0; i < dimensions.length; i++) {
          if (dimensions[i] >= 0) {
            target[1 + i] = source[dimensions[i]];
          }
        }
        for (int i = 0; i < metrics.length; i++) {
          target[1 + dimensions.length + i] = metrics[i].get(values[i]);
        }
        return target;
      }));
    }));
  }

  private void populate(Object[] values, BiFunction<MergeKey, Object[], Object[]> populator)
  {
    final int[] mvx = mvs(values, mvs);
    if (mvx.length == 0) {
      mapping.compute(new MergeKey(values, indices), populator);
    } else {
      _populate(values, populator, mvx, 0);
    }
  }

  private void _populate(Object[] values, BiFunction<MergeKey, Object[], Object[]> populator, int[] mvx, int x)
  {
    if (x == mvx.length) {
      mapping.compute(new MergeKey(values, indices), populator);
    } else {
      final List list = (List) values[mvx[x]];
      final int size = list.size();
      if (size == 1) {
        values[mvx[x]] = list.get(0);
        _populate(values, populator, mvx, x + 1);
      } else {
        for (int i = 0; i < size; i++) {
          Object[] copy = Arrays.copyOf(values, values.length);
          copy[mvx[x]] = list.get(i);
          _populate(copy, populator, mvx, x + 1);
        }
      }
    }
  }

  private static final int[] NONE = new int[0];
  private static final int[] SKRATCH = new int[16];

  private static int[] mvs(Object[] values, int[] mvs)
  {
    if (mvs.length == 0) {
      return NONE;
    }
    // damn slow : IntStream.range(0, mvs.length).filter(x -> values[x] instanceof List).toArray()
    int size = 0;
    for (int i = 0; i < mvs.length; i++) {
      if (values[mvs[i]] instanceof List) {
        SKRATCH[size++] = mvs[i];
      }
    }
    return size == 0 ? NONE : Arrays.copyOf(SKRATCH, size);
  }

  private static final Comparator comparator = GuavaUtils.nullFirstNatural();

  private static final class MergeKey implements Comparable<MergeKey>
  {
    private final Object[] values;
    private final int[] indices;

    private MergeKey(Object[] values, int[] indices)
    {
      this.values = values;
      this.indices = indices;
    }

    @Override
    public boolean equals(Object o)
    {
      MergeKey other = (MergeKey) o;
      for (int i = 0; i < indices.length; i++) {
        if (!Objects.equals(values[indices[i]], other.values[indices[i]])) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode()
    {
      int hash = 1;
      for (int i = 0; i < indices.length; i++) {
        hash = 31 * hash + Objects.hashCode(values[indices[i]]);
      }
      return hash;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(final MergeKey o)
    {
      int compare = 0;
      for (int i = 0; compare == 0 && i < indices.length; i++) {
        compare = comparator.compare(values[indices[i]], o.values[indices[i]]);
      }
      return compare;
    }
  }
}
