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

package io.druid.query.aggregation;

import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory.Combiner;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class Aggregators
{
  public static Aggregator[] makeAggregators(List<AggregatorFactory> factories, ColumnSelectorFactory factory)
  {
    Aggregator[] aggregators = new Aggregator[factories.size()];
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i] = factories.get(i).factorize(factory);
    }
    return aggregators;
  }

  @SuppressWarnings("unchecked")
  public static Object[] aggregate(Object[] values, Aggregator[] aggregators)
  {
    for (int i = 0; i < aggregators.length; i++) {
      values[i] = aggregators[i].aggregate(values[i]);
    }
    return values;
  }

  @SuppressWarnings("unchecked")
  public static Object[] get(Object[] values, Aggregator[] aggregators)
  {
    for (int i = 0; i < aggregators.length; i++) {
      values[i] = aggregators[i].get(values[i]);
    }
    return values;
  }

  @SuppressWarnings("unchecked")
  public static void close(Aggregator[] aggregators)
  {
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].close();
    }
  }

  public static Aggregator noopAggregator()
  {
    return Aggregator.NULL;
  }

  @SuppressWarnings("unchecked")
  public static class DelegatedAggregator<T> implements Aggregator<T>
  {
    final Aggregator<T> delegate;

    public DelegatedAggregator(Aggregator<T> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public T aggregate(T current)
    {
      return delegate.aggregate(current);
    }

    @Override
    public Object get(T current)
    {
      return delegate.get(current);
    }

    @Override
    public void close()
    {
      delegate.close();
    }
  }

  public static BufferAggregator noopBufferAggregator()
  {
    return new BufferAggregator()
    {
      @Override
      public void init(ByteBuffer buf, int position)
      {
      }

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
      }

      @Override
      public Object get(ByteBuffer buf, int position)
      {
        return null;
      }

      @Override
      public void close()
      {
      }
    };
  }

  public static class DelegatedBufferAggregator implements BufferAggregator
  {
    private final BufferAggregator delegate;

    public DelegatedBufferAggregator(BufferAggregator delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
      delegate.init(buf, position);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      delegate.aggregate(buf, position);
    }

    @Override
    public Object get(ByteBuffer buf, int position)
    {
      return delegate.get(buf, position);
    }

    @Override
    public void close()
    {
      delegate.close();
    }
  }

  public static enum RELAY_TYPE
  {
    ONLY_ONE, FIRST, LAST, MIN, MAX, TIME_MIN, TIME_MAX;

    public static RELAY_TYPE fromString(String value)
    {
      return value == null ? ONLY_ONE : valueOf(value.toUpperCase());
    }
  }

  public static Aggregator relayAggregator(ColumnSelectorFactory factory, String column, String type)
  {
    return relayAggregator(factory, column, RELAY_TYPE.fromString(type));
  }

  public static Aggregator relayAggregator(
      final ColumnSelectorFactory factory,
      final String column,
      final RELAY_TYPE type
  )
  {
    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(column);
    if (selector == null) {
      return noopAggregator();
    }
    switch (type) {
      case ONLY_ONE:
        return new Aggregator.Abstract()
        {
          @Override
          public Object aggregate(Object current)
          {
            if (current != null) {
              throw new IllegalStateException("cannot aggregate");
            }
            return selector.get();
          }
        };
      case FIRST:
        return new Aggregator.Abstract()
        {
          @Override
          public Object aggregate(Object current)
          {
            return current == null ? selector.get() : current;
          }
        };
      case LAST:
        return new Aggregator.Abstract()
        {
          @Override
          public Object aggregate(Object current)
          {
            return selector.get();
          }
        };
      case MIN:
        return new Aggregator.Abstract()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Object aggregate(Object current)
          {
            final Object update = selector.get();
            if (update == null) {
              return current;
            }
            if (current == null || GuavaUtils.NULL_FIRST_NATURAL.compare(current, update) > 0) {
              current = update;
            }
            return current;
          }
        };
      case MAX:
        return new Aggregator.Abstract()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Object aggregate(Object current)
          {
            final Object update = selector.get();
            if (update == null) {
              return current;
            }
            if (current == null || GuavaUtils.NULL_FIRST_NATURAL.compare(current, update) < 0) {
              current = update;
            }
            return current;
          }
        };
      case TIME_MIN:
        return new Aggregator.Abstract<TimeTagged>()
        {
          final LongColumnSelector timeSelector = factory.makeLongColumnSelector(Row.TIME_COLUMN_NAME);

          @Override
          @SuppressWarnings("unchecked")
          public TimeTagged aggregate(TimeTagged current)
          {
            final long timestamp = timeSelector.get();
            if (current == null) {
              current = new TimeTagged(timestamp, selector.get());
            } else  if (Longs.compare(timestamp, current.timestamp) < 0) {
              current.timestamp = timestamp;
              current.value = selector.get();
            }
            return current;
          }

          @Override
          public Object get(TimeTagged current)
          {
            return current == null ? null : Arrays.asList(current.timestamp, current.value);
          }
        };
      case TIME_MAX:
        return new Aggregator.Abstract<TimeTagged>()
        {
          final LongColumnSelector timeSelector = factory.makeLongColumnSelector(Row.TIME_COLUMN_NAME);

          @Override
          @SuppressWarnings("unchecked")
          public TimeTagged aggregate(TimeTagged current)
          {
            final long timestamp = timeSelector.get();
            if (current == null) {
              current = new TimeTagged(timestamp, selector.get());
            } else  if (Longs.compare(timestamp, current.timestamp) > 0) {
              current.timestamp = timestamp;
              current.value = selector.get();
            }
            return current;
          }

          @Override
          public Object get(TimeTagged current)
          {
            return current == null ? null : Arrays.asList(current.timestamp, current.value);
          }
        };
      default:
        throw new IllegalArgumentException("invalid type " + type);
    }
  }

  private static class TimeTagged
  {
    long timestamp;
    Object value;

    public TimeTagged(long timestamp, Object value)
    {
      this.timestamp = timestamp;
      this.value = value;
    }
  }

  public static BufferAggregator relayBufferAggregator(
      final ColumnSelectorFactory factory,
      final String column,
      final String type
  )
  {
    return new RelayBufferAggregator(relayAggregator(factory, column, type));
  }

  @SuppressWarnings("unchecked")
  public static class RelayBufferAggregator implements BufferAggregator
  {
    private final Aggregator aggregator;
    private final Map<IntArray, Object> mapping = Maps.newHashMap();

    public RelayBufferAggregator(Aggregator aggregator)
    {
      this.aggregator = aggregator;
    }

    private Aggregators.IntArray toKey(ByteBuffer buf, int position)
    {
      return new IntArray(new int[] {System.identityHashCode(buf), position});
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
      mapping.remove(toKey(buf, position));
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      final IntArray key = toKey(buf, position);
      final Object current = mapping.get(key);
      final Object updated = aggregator.aggregate(current);
      if (current != updated) {
        mapping.put(key, updated);
      }
    }

    @Override
    public Object get(ByteBuffer buf, int position)
    {
      return aggregator.get(mapping.get(toKey(buf, position)));
    }

    @Override
    public void close()
    {
      mapping.clear();
      aggregator.close();
    }
  }

  @SuppressWarnings("unchecked")
  public static Combiner relayCombiner(String type)
  {
    switch (RELAY_TYPE.fromString(type)) {
      case ONLY_ONE:
        return new Combiner.Abstract()
        {
          @Override
          public Object _combine(Object param1, Object param2)
          {
            throw new UnsupportedOperationException("cannot combine");
          }
        };
      case FIRST:
        return new Combiner()
        {
          @Override
          public Object combine(Object param1, Object param2)
          {
            return param1 == null ? param2 : param1;
          }
        };
      case LAST:
        return new Combiner()
        {
          @Override
          public Object combine(Object param1, Object param2)
          {
            return param2 == null ? param1 : param2;
          }
        };
      case MIN:
        return new Combiner()
        {
          @Override
          public Object combine(Object param1, Object param2)
          {
            return GuavaUtils.NULL_FIRST_NATURAL.compare(param1, param2) < 0 ? param1 : param2;
          }
        };
      case MAX:
        return new Combiner()
        {
          @Override
          public Object combine(Object param1, Object param2)
          {
            return GuavaUtils.NULL_FIRST_NATURAL.compare(param1, param2) > 0 ? param1 : param2;
          }
        };
      case TIME_MIN:
        return new Combiner.Abstract()
        {
          @Override
          public Object _combine(Object param1, Object param2)
          {
            final Number time1 = (Number) ((List) param1).get(0);
            final Number time2 = (Number) ((List) param2).get(0);
            return Longs.compare(time1.longValue(), time2.longValue()) < 0 ? param1 : param2;
          }
        };
      case TIME_MAX:
        return new Combiner.Abstract()
        {
          @Override
          public Object _combine(Object param1, Object param2)
          {
            final Number time1 = (Number) ((List) param1).get(0);
            final Number time2 = (Number) ((List) param2).get(0);
            return Longs.compare(time1.longValue(), time2.longValue()) > 0 ? param1 : param2;
          }
        };
      default:
        throw new IllegalArgumentException("invalid type " + type);
    }
  }

  private static class IntArray implements Comparable<IntArray>
  {
    private final int[] array;

    private IntArray(int[] array)
    {
      this.array = array;
    }

    @Override
    public int hashCode()
    {
      return array.length == 1 ? array[0] : Arrays.hashCode(array);
    }

    @Override
    public boolean equals(Object obj)
    {
      final int[] other = ((IntArray) obj).array;
      return array.length == 1 ? array[0] == other[0] : Arrays.equals(array, other);
    }

    @Override
    public int compareTo(IntArray o)
    {
      for (int i = 0; i < array.length; i++) {
        final int compare = Integer.compare(array[i], o.array[i]);
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    }
  }

  public static interface EstimableAggregator<T> extends Aggregator<T>
  {
    int estimateOccupation(T current);
  }

  public static abstract class AbstractEstimableAggregator<T> extends Aggregator.Abstract<T>
      implements EstimableAggregator<T>
  {
  }
}
