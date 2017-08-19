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
import io.druid.data.ValueDesc;
import io.druid.segment.ColumnSelectorFactories.FixedArrayIndexed;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

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
      public ValueDesc type()
      {
        return selector.type();
      }

      @Override
      public List get()
      {
        return holder.get();
      }
    };

    return new Aggregators.EstimableAggregator()
    {
      private int estimated = 128;
      private final List<Aggregator> aggregators = Lists.newArrayList();

      @Override
      public int estimateOccupation()
      {
        return estimated;
      }

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
          Aggregator factorize = delegate.factorize(new FixedArrayIndexed(i, memoized, elementType));
          if (factorize instanceof Aggregators.EstimableAggregator) {
            estimated += ((Aggregators.EstimableAggregator)factorize).estimateOccupation();
          } else {
            estimated += delegate.getMaxIntermediateSize();
          }
          estimated += 32;
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

    final DSuppliers.HandOver<List> holder = new DSuppliers.HandOver<>();
    final ObjectColumnSelector<List> memoized = new ObjectColumnSelector<List>()
    {
      @Override
      public ValueDesc type()
      {
        return selector.type();
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
          BufferAggregator factorize = delegate.factorizeBuffered(new FixedArrayIndexed(i, memoized, elementType));
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

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    ArrayAggregatorFactory array = checkMergeable(other);
    if (!Objects.equals(column, array.column)) {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
    return new ArrayAggregatorFactory(column, delegate.getMergingFactory(array.delegate), Math.max(limit, array.limit));
  }
}
