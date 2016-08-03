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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.ExprEvalColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class ArrayAggregatorFactory extends AbstractArrayAggregatorFactory
{
  public ArrayAggregatorFactory(
      @JsonProperty("column") String column,
      @JsonProperty("aggregator") AggregatorFactory delegate,
      @JsonProperty("limit") int limit
  )
  {
    super(column, delegate, limit);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(column);

    return new Aggregator()
    {
      private final List<Aggregator> aggregators = Lists.newArrayList();

      @Override
      public void aggregate()
      {
        List value = selector.get();
        for (Aggregator aggregator : getAggregators(value.size())) {
          aggregator.aggregate();
        }
      }

      @Override
      public void reset()
      {
        for (Aggregator aggregator : aggregators) {
          aggregator.reset();
        }
      }

      @Override
      public Object get()
      {
        List<Object> result = Lists.newArrayListWithExpectedSize(aggregators.size());
        for (Aggregator aggregator : aggregators) {
          result.add(aggregator.get());
        }
        return result;
      }

      @Override
      public long getLong()
      {
        throw new UnsupportedOperationException("getLong");
      }

      @Override
      public float getFloat()
      {
        throw new UnsupportedOperationException("getFloat");
      }

      @Override
      public double getDouble()
      {
        throw new UnsupportedOperationException("getDouble");
      }

      @Override
      public String getName()
      {
        return delegate.getName();
      }

      @Override
      public void close()
      {
        for (Aggregator aggregator : aggregators) {
          aggregator.close();
        }
      }

      private List<Aggregator> getAggregators(int size)
      {
        final int min = Math.min(limit, size);
        for (int i = aggregators.size(); i < min; i++) {
          Aggregator factorize = delegate.factorize(new ArrayColumnSelectorFactory(i, selector, elementClass));
          aggregators.add(factorize);
        }
        return aggregators;
      }
    };
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(column);

    return new BufferAggregator()
    {

      private final List<BufferAggregator> aggregators = Lists.newArrayList();
      private final byte[] clear = new byte[delegate.getMaxIntermediateSize()];

      @Override
      public void init(ByteBuffer buf, int position)
      {
        buf.position(position);
        for (int i = 0; i < limit; i++) {
          buf.put(clear);
        }
      }

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        List value = selector.get();
        for (BufferAggregator aggregator : getAggregators(buf, position, value.size())) {
          aggregator.aggregate(buf, position);
          position += delegate.getMaxIntermediateSize();
        }
      }

      @Override
      public Object get(ByteBuffer buf, int position)
      {
        List<Object> result = Lists.newArrayListWithExpectedSize(aggregators.size());
        for (BufferAggregator aggregator : aggregators) {
          result.add(aggregator.get(buf, position));
          position += delegate.getMaxIntermediateSize();
        }
        return result;
      }

      @Override
      public float getFloat(ByteBuffer buf, int position)
      {
        throw new UnsupportedOperationException("getFloat");
      }

      @Override
      public double getDouble(ByteBuffer buf, int position)
      {
        throw new UnsupportedOperationException("getDouble");
      }

      @Override
      public long getLong(ByteBuffer buf, int position)
      {
        throw new UnsupportedOperationException("getLong");
      }

      @Override
      public void close()
      {
        for (BufferAggregator aggregator : aggregators) {
          aggregator.close();
        }
      }

      private List<BufferAggregator> getAggregators(ByteBuffer buf, int position, int size)
      {
        final int min = Math.min(limit, size);
        for (int i = aggregators.size(); i < min; i++) {
          ArrayColumnSelectorFactory factory = new ArrayColumnSelectorFactory(i, selector, elementClass);
          BufferAggregator factorize = delegate.factorizeBuffered(factory);
          factorize.init(buf, position);
          position += delegate.getMaxIntermediateSize();
          aggregators.add(factorize);
        }
        return aggregators;
      }
    };
  }

  private static final class ArrayColumnSelectorFactory implements ColumnSelectorFactory {

    private final int index;
    private final ObjectColumnSelector selector;
    private final Class elementClass;

    private ArrayColumnSelectorFactory(int index, ObjectColumnSelector selector, Class elementClass)
    {
      this.index = index;
      this.selector = selector;
      this.elementClass = elementClass;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("makeDimensionSelector");
    }

    private Object getObject() {
      List value = (List) selector.get();
      return value == null ? value : value.get(index);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          Object value = getObject();
          if (value == null) {
            return 0.0f;
          }
          if (value instanceof Number) {
            return ((Number)value).floatValue();
          }
          return Float.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public double get()
        {
          Object value = getObject();
          if (value == null) {
            return 0.0d;
          }
          if (value instanceof Number) {
            return ((Number)value).doubleValue();
          }
          return Double.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          Object value = getObject();
          if (value == null) {
            return 0L;
          }
          if (value instanceof Number) {
            return ((Number)value).longValue();
          }
          return Long.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return elementClass;
        }

        @Override
        public Object get()
        {
          return getObject();
        }
      };
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      throw new UnsupportedOperationException("makeMathExpressionSelector");
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      throw new UnsupportedOperationException("getColumnCapabilities");
    }
  }
}
