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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.GuavaUtils;
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
  @SuppressWarnings("unchecked")
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

    return new Aggregator.Estimable<List<Object>>()
    {
      private final List<Aggregator> aggregators = Lists.newArrayList();

      @Override
      public int estimateOccupation(List<Object> current)
      {
        int estimated = 32;
        for (int i = 0; i < aggregators.size(); i++) {
          Aggregator aggregator = aggregators.get(i);
          if (aggregator instanceof Estimable) {
            estimated += ((Estimable) aggregator).estimateOccupation(current.get(i));
          } else {
            estimated += delegate.getMaxIntermediateSize();
          }
        }
        return estimated;
      }

      @Override
      public List<Object> aggregate(List<Object> current)
      {
        final List value = selector.get();
        if (!GuavaUtils.isNullOrEmpty(value)) {
          holder.set(value);
          if (current == null) {
            current = Lists.newArrayList();
          }
          for (int i = current.size(); i < value.size(); i++) {
            current.add(null);
          }
          final List<Aggregator> aggregators = getAggregators(value.size());
          for (int i = 0; i < aggregators.size(); i++) {
            current.set(i, aggregators.get(i).aggregate(current.get(i)));
          }
        }
        return current;
      }

      @Override
      public Object get(List<Object> current)
      {
        List<Object> result = Lists.newArrayListWithCapacity(aggregators.size());
        for (int i = 0; i < aggregators.size(); i++) {
          result.add(aggregators.get(i).get(current.get(i)));
        }
        return result;
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
          aggregators.add(delegate.factorize(new FixedArrayIndexed(i, memoized, elementType)));
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
