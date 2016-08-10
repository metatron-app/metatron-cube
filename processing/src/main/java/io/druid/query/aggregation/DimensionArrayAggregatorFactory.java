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
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.ExprEvalColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedInts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 */
public class DimensionArrayAggregatorFactory extends AbstractArrayAggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x7E;

  public DimensionArrayAggregatorFactory(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("aggregator") AggregatorFactory delegate,
      @JsonProperty("limit") int limit
  )
  {
    super(dimension, delegate, limit);
  }

  @Override
  protected byte cacheTypeID()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(column));
    return new Aggregator()
    {
      private final List<Aggregator> aggregators = Lists.newArrayList();

      @Override
      public void aggregate()
      {
        IndexedInts dims = selector.getRow();
        List<Aggregator> ready = getAggregators(dims.size());
        for (Aggregator aggregator : ready) {
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
          aggregators.add(delegate.factorize(new DimensionArrayColumnSelectorFactory(i, selector)));
        }
        return aggregators;
      }
    };
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(column));

    return new BufferAggregator()
    {

      private final List<BufferAggregator> aggregators = Lists.newArrayList();

      @Override
      public void init(ByteBuffer buf, int position)
      {
      }

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        IndexedInts dims = selector.getRow();
        for (BufferAggregator aggregator : getAggregators(buf, position, dims.size())) {
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
          BufferAggregator factorize = delegate.factorizeBuffered(new DimensionArrayColumnSelectorFactory(i, selector));
          factorize.init(buf, position);
          position += delegate.getMaxIntermediateSize();
          aggregators.add(factorize);
        }
        return aggregators;
      }
    };
  }

  private static class DimensionArrayColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final int index;
    private final DimensionSelector selector;

    private DimensionArrayColumnSelectorFactory(int index, DimensionSelector selector)
    {
      this.index = index;
      this.selector = selector;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return new DimensionSelector()
      {
        @Override
        public IndexedInts getRow()
        {
          return new IndexedInts()
          {
            @Override
            public int size()
            {
              return 1;
            }

            @Override
            public int get(int index)
            {
              return selector.getRow().get(index);
            }

            @Override
            public void fill(int index, int[] toFill)
            {
              throw new UnsupportedOperationException("fill");
            }

            @Override
            public void close() throws IOException
            {
            }

            @Override
            public Iterator<Integer> iterator()
            {
              return Iterators.singletonIterator(get(0));
            }
          };
        }

        @Override
        public int getValueCardinality()
        {
          return selector.getValueCardinality();
        }

        @Override
        public String lookupName(int id)
        {
          return selector.lookupName(id);
        }

        @Override
        public int lookupId(String name)
        {
          return selector.lookupId(name);
        }
      };
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          final String value = selector.lookupName(selector.getRow().get(index));
          return Strings.isNullOrEmpty(value) ? 0.0f : Float.valueOf(value);
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
          final String value = selector.lookupName(selector.getRow().get(index));
          return Strings.isNullOrEmpty(value) ? 0.0d : Double.valueOf(value);
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
          final String value = selector.lookupName(selector.getRow().get(index));
          return Strings.isNullOrEmpty(value) ? 0L : Long.valueOf(value);
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
          return String.class;
        }

        @Override
        public Object get()
        {
          return selector.lookupName(selector.getRow().get(index));
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
