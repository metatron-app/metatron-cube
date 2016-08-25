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
import io.druid.common.guava.DSuppliers;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class ArrayAggregatorFactory extends AbstractArrayAggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x7F;

  public ArrayAggregatorFactory(
      @JsonProperty("column") String column,
      @JsonProperty("aggregator") AggregatorFactory delegate,
      @JsonProperty("limit") int limit
  )
  {
    super(column, delegate, limit);
  }

  @Override
  protected byte cacheTypeID()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(column);

    final DSuppliers.HandOver<List> holder = new DSuppliers.HandOver<>();
    final ObjectColumnSelector<List> memoized = new ObjectColumnSelector<List>()
    {
      @Override
      public Class classOfObject()
      {
        return selector.classOfObject();
      }

      @Override
      public List get()
      {
        return holder.get();
      }
    };

    return new Aggregators.MutableSizedAggregator()
    {
      private final List<Aggregator> aggregators = Lists.newArrayList();

      @Override
      public void aggregate()
      {
        final List value = selector.get();
        if (value != null && !value.isEmpty()) {
          holder.set(value);
          for (Aggregator aggregator : getAggregators(value.size())) {
            aggregator.aggregate();
          }
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
        List<Object> result = Lists.newArrayListWithCapacity(aggregators.size());
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
          aggregators.add(delegate.factorize(new ColumnSelectorFactories.FixedArrayIndexed(i, memoized, elementClass)));
          increment(delegate.getMaxIntermediateSize());
        }
        return aggregators;
      }
    };
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(column);

    final DSuppliers.HandOver<List> holder = new DSuppliers.HandOver<>();
    final ObjectColumnSelector<List> memoized = new ObjectColumnSelector<List>()
    {
      @Override
      public Class classOfObject()
      {
        return selector.classOfObject();
      }

      @Override
      public List get()
      {
        return holder.get();
      }
    };

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
        final List value = selector.get();
        if (value != null && !value.isEmpty()) {
          holder.set(value);
          for (BufferAggregator aggregator : getAggregators(buf, position, value.size())) {
            aggregator.aggregate(buf, position);
            position += delegate.getMaxIntermediateSize();
          }
        }
      }

      @Override
      public Object get(ByteBuffer buf, int position)
      {
        List<Object> result = Lists.newArrayListWithCapacity(aggregators.size());
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
          BufferAggregator factorize = delegate.factorizeBuffered(
              new ColumnSelectorFactories.FixedArrayIndexed(i, memoized, elementClass)
          );
          factorize.init(buf, position);
          position += delegate.getMaxIntermediateSize();
          aggregators.add(factorize);
        }
        return aggregators;
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new ArrayAggregatorFactory(delegate.getName(), delegate, limit);
  }
}
