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

package io.druid.query.aggregation;

import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import io.druid.common.guava.GuavaUtils;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

/**
 */
public class Aggregators
{
  public static Aggregator noopAggregator()
  {
    return new Aggregator()
    {
      @Override
      public void aggregate()
      {

      }

      @Override
      public void reset()
      {

      }

      @Override
      public Object get()
      {
        return null;
      }

      @Override
      public float getFloat()
      {
        return 0;
      }

      @Override
      public void close()
      {

      }

      @Override
      public long getLong()
      {
        return 0;
      }

      @Override
      public double getDouble()
      {
        return 0D;
      }
    };
  }

  public static class DelegatedAggregator implements Aggregator
  {
    private final Aggregator delegate;

    public DelegatedAggregator(Aggregator delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public void aggregate()
    {
      delegate.aggregate();
    }

    @Override
    public void reset()
    {
      delegate.reset();
    }

    @Override
    public Object get()
    {
      return delegate.get();
    }

    @Override
    public float getFloat()
    {
      return delegate.getFloat();
    }

    @Override
    public void close()
    {
      delegate.close();
    }

    @Override
    public long getLong()
    {
      return delegate.getLong();
    }

    @Override
    public double getDouble()
    {
      return delegate.getDouble();
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
      public float getFloat(ByteBuffer buf, int position)
      {
        return 0;
      }

      @Override
      public double getDouble(ByteBuffer buf, int position)
      {
        return 0D;
      }


      @Override
      public long getLong(ByteBuffer buf, int position)
      {
        return 0L;
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
    public float getFloat(ByteBuffer buf, int position)
    {
      return delegate.getFloat(buf, position);
    }

    @Override
    public double getDouble(ByteBuffer buf, int position)
    {
      return delegate.getDouble(buf, position);
    }

    @Override
    public long getLong(ByteBuffer buf, int position)
    {
      return delegate.getLong(buf, position);
    }

    @Override
    public void close()
    {
      delegate.close();
    }
  }

  public static enum RELAY_TYPE
  {
    ONLY_ONE, FIRST, LAST, MIN, MAX;

    public static RELAY_TYPE fromString(String value)
    {
      return value == null ? ONLY_ONE : valueOf(value.toUpperCase());
    }
  }

  public static Aggregator relayAggregator(final ObjectColumnSelector selector, final String type)
  {
    return relayAggregator(selector, RELAY_TYPE.fromString(type));
  }

  public static Aggregator relayAggregator(final ObjectColumnSelector selector, final RELAY_TYPE type)
  {
    switch (type) {
      case ONLY_ONE:
        return new RelayAggregator()
        {
          @Override
          public void aggregate()
          {
            if (selected) {
              throw new IllegalStateException("cannot aggregate");
            }
            update(selector);
          }
        };
      case FIRST:
        return new RelayAggregator()
        {
          @Override
          public void aggregate()
          {
            if (!selected) {
              update(selector);
            }
          }
        };
      case LAST:
        return new RelayAggregator()
        {
          @Override
          public void aggregate()
          {
            update(selector);
          }
        };
      case MIN:
        return new RelayAggregator()
        {
          @Override
          @SuppressWarnings("unchecked")
          public void aggregate()
          {
            final Object update = selector.get();
            if (!selected || GuavaUtils.NULL_FIRST_NATURAL.compare(value, update) > 0) {
              selected = true;
              value = update;
            }
          }
        };
      case MAX:
        return new RelayAggregator()
        {
          @Override
          @SuppressWarnings("unchecked")
          public void aggregate()
          {
            final Object update = selector.get();
            if (!selected || GuavaUtils.NULL_FIRST_NATURAL.compare(value, update) < 0) {
              selected = true;
              value = update;
            }
          }
        };
      default:
        throw new IllegalArgumentException("invalid type " + type);
    }
  }

  private static abstract class RelayAggregator implements Aggregator
  {
    boolean selected;
    Object value;

    @Override
    public void reset()
    {
      selected = false;
      value = null;
    }

    protected final void update(final ObjectColumnSelector selector)
    {
      selected = true;
      value = selector.get();
    }

    @Override
    public Object get()
    {
      return value;
    }

    @Override
    public float getFloat()
    {
      if (value == null) {
        return 0;
      }
      if (value instanceof Number) {
        return ((Number) value).floatValue();
      }
      if (value instanceof String) {
        Long longValue = Longs.tryParse((String) value);
        if (longValue != null) {
          return longValue.floatValue();
        }
        return Float.valueOf((String) value);
      }
      throw new IllegalArgumentException("cannot convert " + value.getClass() + " to float");
    }

    @Override
    public long getLong()
    {
      if (value == null) {
        return 0;
      }
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
      if (value instanceof String) {
        Long longValue = Longs.tryParse((String) value);
        if (longValue != null) {
          return longValue;
        }
        return Long.valueOf((String) value);
      }
      throw new IllegalArgumentException("cannot convert " + value.getClass() + " to long");
    }

    @Override
    public double getDouble()
    {
      if (value == null) {
        return 0;
      }
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
      if (value instanceof String) {
        Long longValue = Longs.tryParse((String) value);
        if (longValue != null) {
          return longValue.doubleValue();
        }
        return Double.valueOf((String) value);
      }
      throw new IllegalArgumentException("cannot convert " + value.getClass() + " to double");
    }

    @Override
    public void close() {}
  }

  public static BufferAggregator relayBufferAggregator(final ObjectColumnSelector selector, final String type)
  {
    return new RelayBufferAggregator()
    {
      private final RELAY_TYPE relayType = RELAY_TYPE.fromString(type);
      @Override
      protected Aggregator newAggregator()
      {
        return relayAggregator(selector, relayType);
      }
    };
  }

  private static abstract class RelayBufferAggregator extends BufferAggregator.Abstract
  {
    private final Map<IntArray, Aggregator> mapping = Maps.newHashMap();

    private Aggregators.IntArray toKey(ByteBuffer buf, int position)
    {
      return new IntArray(new int[] {System.identityHashCode(buf), position});
    }

    protected abstract Aggregator newAggregator();

    @Override
    public void init(ByteBuffer buf, int position)
    {
      mapping.put(toKey(buf, position), newAggregator());
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      mapping.get(toKey(buf, position)).aggregate();
    }

    @Override
    public Object get(ByteBuffer buf, int position)
    {
      return mapping.get(toKey(buf, position)).get();
    }

    @Override
    public float getFloat(ByteBuffer buf, int position)
    {
      return mapping.get(toKey(buf, position)).getFloat();
    }

    @Override
    public double getDouble(ByteBuffer buf, int position)
    {
      return mapping.get(toKey(buf, position)).getDouble();
    }

    @Override
    public long getLong(ByteBuffer buf, int position)
    {
      return mapping.get(toKey(buf, position)).getLong();
    }

    @Override
    public void close()
    {
      mapping.clear();
    }
  }

  @SuppressWarnings("unchecked")
  public static AggregatorFactory.Combiner relayCombiner(String type)
  {
    switch (RELAY_TYPE.fromString(type)) {
      case ONLY_ONE:
        return new AggregatorFactory.Combiner()
        {
          @Override
          public Object combine(Object param1, Object param2)
          {
            if (param1 == null) {
              return param2;
            } else if (param2 == null) {
              return param1;
            }
            throw new UnsupportedOperationException("cannot combine");
          }
        };
      case FIRST:
        return new AggregatorFactory.Combiner()
        {
          @Override
          public Object combine(Object param1, Object param2)
          {
            return param1 == null ? param2 : param1;
          }
        };
      case LAST:
        return new AggregatorFactory.Combiner()
        {
          @Override
          public Object combine(Object param1, Object param2)
          {
            return param2 == null ? param1 : param2;
          }
        };
      case MIN:
        return new AggregatorFactory.Combiner()
        {
          @Override
          public Object combine(Object param1, Object param2)
          {
            return GuavaUtils.NULL_FIRST_NATURAL.compare(param1, param2) < 0 ? param1 : param2;
          }
        };
      case MAX:
        return new AggregatorFactory.Combiner()
        {
          @Override
          public Object combine(Object param1, Object param2)
          {
            return GuavaUtils.NULL_FIRST_NATURAL.compare(param1, param2) > 0 ? param1 : param2;
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

  public static interface EstimableAggregator extends Aggregator
  {
    int estimateOccupation();
  }

  public static abstract class AbstractEstimableAggregator extends Aggregator.Abstract implements EstimableAggregator
  {
  }
}
