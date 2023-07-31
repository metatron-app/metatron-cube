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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.IOUtils;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.Aggregator.Streaming;
import io.druid.query.aggregation.Aggregator.Vectorized;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.lang.mutable.MutableLong;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 */
public class Aggregators
{
  public static boolean isCountAll(List<AggregatorFactory> factories)
  {
    return factories.size() == 1 && isCountAll(factories.get(0));
  }

  public static boolean isCountAll(AggregatorFactory factory)
  {
    if (factory instanceof CountAggregatorFactory) {
      CountAggregatorFactory counter = (CountAggregatorFactory) factory;
      return counter.getFieldName() == null && counter.getPredicate() == null;
    }
    return false;
  }

  public static Aggregator[] makeAggregators(List<AggregatorFactory> factories, ColumnSelectorFactory factory)
  {
    return makeAggregators(factories, factory, false);
  }

  public static Aggregator[] makeAggregators(
      List<AggregatorFactory> factories,
      ColumnSelectorFactory factory,
      boolean streaming
  )
  {
    Aggregator[] aggregators = new Aggregator[factories.size()];
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i] = factories.get(i).factorize(factory);
      if (streaming && aggregators[i] instanceof Aggregator.StreamingSupport) {
        aggregators[i] = ((Aggregator.StreamingSupport) aggregators[i]).streaming();
      }
    }
    return aggregators;
  }

  public static Streaming[] makeStreaming(List<AggregatorFactory> factories, ColumnSelectorFactory factory)
  {
    Streaming[] aggregators = new Streaming[factories.size()];
    for (int i = 0; i < aggregators.length; i++) {
      Aggregator aggregator = factories.get(i).factorize(factory);
      if (!(aggregator instanceof Aggregator.Streaming) || !((Streaming) aggregator).supports()) {
        return null;
      }
      aggregators[i] = (Streaming) aggregator;
    }
    return aggregators;
  }

  public static boolean allVectorizable(List<AggregatorFactory> factories)
  {
    return factories.stream().allMatch(f -> f instanceof AggregatorFactory.Vectorizable);
  }

  public static Vectorized[] vectorize(List<AggregatorFactory> factories, ColumnSelectorFactory factory)
  {
    Vectorized[] aggregators = new Vectorized[factories.size()];
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i] = ((AggregatorFactory.Vectorizable) factories.get(i)).create(factory);
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

  public static <T> BiFunction<T, Object[], Object[]> populator(Aggregator[] metrics)
  {
    return (key, prev) -> Aggregators.aggregate(prev == null ? new Object[metrics.length] : prev, metrics);
  }

  @SuppressWarnings("unchecked")
  public static Object[] get(Object[] values, Aggregator[] aggregators)
  {
    for (int i = 0; i < aggregators.length; i++) {
      values[i] = aggregators[i].get(values[i]);
    }
    return values;
  }

  public static void close(Aggregator[] aggregators)
  {
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].clear(true);
    }
  }

  public static void close(Vectorized[] aggregators)
  {
    for (int i = 0; i < aggregators.length; i++) {
      IOUtils.closeQuietly(aggregators[i]);
    }
  }

  public static enum RelayType
  {
    ONLY_ONE, FIRST, LAST, MIN, MAX, TIME_MIN, TIME_MAX;

    @JsonValue
    public String getName()
    {
      return name();
    }

    // see ColumnStats
    public String toStatKey()
    {
      return name().toLowerCase();
    }

    @JsonCreator
    public static RelayType fromString(String value)
    {
      return value == null ? ONLY_ONE : valueOf(value.toUpperCase());
    }
  }

  public static Aggregator relayAggregator(
      final ColumnSelectorFactory factory,
      final String column,
      final RelayType type
  )
  {
    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(column);
    if (selector == null) {
      return Aggregator.NULL;
    }
    switch (type) {
      case ONLY_ONE:
        return new Aggregator.Simple()
        {
          @Override
          public Object aggregate(Object current)
          {
            final Object update = selector.get();
            if (update != null && current != null) {
              throw new IllegalStateException("cannot aggregate");
            }
            return update == null ? current : update;
          }
        };
      case MIN:
        return new Aggregator.Simple()
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
        return new Aggregator.Simple()
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
      case FIRST:
      case TIME_MIN:
        return new Aggregator<TimeTagged>()
        {
          private final MutableLong handover = new MutableLong();
          private final LongColumnSelector timeSelector = factory.makeLongColumnSelector(Row.TIME_COLUMN_NAME);

          @Override
          public TimeTagged aggregate(TimeTagged current)
          {
            if (!timeSelector.getLong(handover)) {
              return current; // possible?
            }
            final long timestamp = handover.longValue();
            if (current == null) {
              Object value = selector.get();
              if (value != null) {
                current = new TimeTagged(timestamp, value);
              }
            } else if (timestamp < current.timestamp) {
              Object value = selector.get();
              if (value != null) {
                current.timestamp = timestamp;
                current.value = value;
              }
            }
            return current;
          }

          @Override
          public Object get(TimeTagged current)
          {
            return current == null ? null : Arrays.asList(current.timestamp, current.value);
          }
        };
      case LAST:
      case TIME_MAX:
        return new Aggregator<TimeTagged>()
        {
          private final MutableLong handover = new MutableLong();
          private final LongColumnSelector timeSelector = factory.makeLongColumnSelector(Row.TIME_COLUMN_NAME);

          @Override
          public TimeTagged aggregate(TimeTagged current)
          {
            if (!timeSelector.getLong(handover)) {
              return current; // possible?
            }
            final long timestamp = handover.longValue();
            if (current == null) {
              Object value = selector.get();
              if (value != null) {
                current = new TimeTagged(timestamp, value);
              }
            } else if (timestamp > current.timestamp) {
              Object value = selector.get();
              if (value != null) {
                current.timestamp = timestamp;
                current.value = value;
              }
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
      final RelayType type
  )
  {
    return new RelayBufferAggregator(relayAggregator(factory, column, type));
  }

  public static class BufferMapping<T>
  {
    protected final Map<Key, T> mapping = Maps.newHashMap();

    protected final Key toKey(int position0, int position1)
    {
      return new Key(position0, position1);
    }

    protected final T get(int position0, int position1)
    {
      return mapping.get(toKey(position0, position1));
    }

    protected final T put(int position0, int position1, T value)
    {
      return mapping.put(toKey(position0, position1), value);
    }

    protected final T remove(int position0, int position1)
    {
      return mapping.remove(toKey(position0, position1));
    }

    protected static final class Key
    {
      private final int position0;
      private final int position1;

      public Key(int position0, int position1)
      {
        this.position0 = position0;
        this.position1 = position1;
      }

      @Override
      public int hashCode()
      {
        return 31 * position0 + position1;
      }

      @Override
      public boolean equals(Object obj)
      {
        Key other = (Key) obj;
        return position0 == other.position0 && position1 == other.position1;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static class RelayBufferAggregator extends BufferMapping implements BufferAggregator
  {
    private final Aggregator aggregator;

    public RelayBufferAggregator(Aggregator aggregator)
    {
      this.aggregator = aggregator;
    }

    @Override
    public void init(ByteBuffer buf, int position0, int position1)
    {
      remove(position0, position1);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position0, int position1)
    {
      final Key key = toKey(position0, position1);
      final Object current = mapping.get(key);
      final Object updated = aggregator.aggregate(current);
      if (current != updated) {
        mapping.put(key, updated);
      }
    }

    @Override
    public Object get(ByteBuffer buf, int position0, int position1)
    {
      return aggregator.get(get(position0, position1));
    }

    @Override
    public void clear(boolean close)
    {
      mapping.clear();
      aggregator.clear(close);
    }
  }

  @SuppressWarnings("unchecked")
  public static BinaryFn.Identical relayCombiner(RelayType type)
  {
    switch (type) {
      case ONLY_ONE:
        return (param1, param2) -> {
          throw new UnsupportedOperationException("cannot combine");
        };
      case MIN:
        return (param1, param2) -> GuavaUtils.NULL_FIRST_NATURAL.compare(param1, param2) < 0 ? param1 : param2;
      case MAX:
        return (param1, param2) -> GuavaUtils.NULL_FIRST_NATURAL.compare(param1, param2) > 0 ? param1 : param2;
      case FIRST:
      case TIME_MIN:
        return (param1, param2) -> {
          final Number time1 = (Number) ((List) param1).get(0);
          final Number time2 = (Number) ((List) param2).get(0);
          return time1.longValue() < time2.longValue() ? param1 : param2;
        };
      case LAST:
      case TIME_MAX:
        return (param1, param2) -> {
          final Number time1 = (Number) ((List) param1).get(0);
          final Number time2 = (Number) ((List) param2).get(0);
          return time1.longValue() > time2.longValue() ? param1 : param2;
        };
      default:
        throw new IllegalArgumentException("invalid type " + type);
    }
  }

  @SuppressWarnings("unchecked")
  public static Aggregator.Simple asAggregator(final BinaryFn.Identical combiner, final ObjectColumnSelector selector)
  {
    return current -> combiner.apply(current, selector.get());
  }

  public static <T> Aggregator<T> wrap(ValueMatcher matcher, Aggregator<T> aggregator)
  {
    return matcher == null || matcher == ValueMatcher.TRUE ? aggregator : new FilteredAggregator<T>(aggregator, matcher);
  }

  public static BufferAggregator wrap(ValueMatcher matcher, BufferAggregator aggregator)
  {
    return matcher == null || matcher == ValueMatcher.TRUE ? aggregator : new FilteredBufferAggregator(aggregator, matcher);
  }

  private static class FilteredAggregator<T> implements Aggregator<T>
  {
    private final Aggregator<T> delegate;
    private final ValueMatcher matcher;

    private FilteredAggregator(Aggregator<T> delegate, ValueMatcher matcher)
    {
      this.delegate = delegate;
      this.matcher = matcher;
    }

    @Override
    public T aggregate(T current)
    {
      return matcher.matches() ? delegate.aggregate(current) : current;
    }

    @Override
    public Object get(T current)
    {
      return delegate.get(current);
    }

    @Override
    public void clear(boolean close)
    {
      delegate.clear(close);
    }
  }

  private static class FilteredBufferAggregator implements BufferAggregator
  {
    private final BufferAggregator delegate;
    private final ValueMatcher matcher;

    public FilteredBufferAggregator(BufferAggregator delegate, ValueMatcher matcher)
    {
      this.delegate = delegate;
      this.matcher = matcher;
    }

    @Override
    public void init(ByteBuffer buf, int position0, int position1)
    {
      delegate.init(buf, position0, position1);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position0, int position1)
    {
      if (matcher.matches()) {
        delegate.aggregate(buf, position0, position1);
      }
    }

    @Override
    public Object get(ByteBuffer buf, int position0, int position1)
    {
      return delegate.get(buf, position0, position1);
    }

    @Override
    public void clear(boolean close)
    {
      delegate.clear(close);
    }
  }
}
