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
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DelegatedDimensionSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

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
  @SuppressWarnings("unchecked")
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(column));
    return new Aggregator.Estimable<List<Object>>()
    {
      private final List<Aggregator> aggregators = Lists.newArrayList();

      @Override
      public int estimateOccupation(List<Object> current)
      {
        int estimated = 32;
        for (int i = 0; i < aggregators.size(); i++) {
          Aggregator aggregator = aggregators.get(i);
          if (aggregator instanceof Aggregator.Estimable) {
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
        IndexedInts dims = selector.getRow();
        if (dims.size() > 0) {
          if (current == null) {
            current = Lists.newArrayList();
          }
          for (int i = current.size(); i < dims.size(); i++) {
            current.add(null);
          }
          final List<Aggregator> aggregators = getAggregators(dims.size());
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
      public void clear(boolean close)
      {
        for (Aggregator aggregator : aggregators) {
          aggregator.clear(close);
        }
      }

      private List<Aggregator> getAggregators(int size)
      {
        final int min = Math.min(limit, size);
        for (int i = aggregators.size(); i < min; i++) {
          Aggregator factorize = delegate.factorize(new DimensionArrayColumnSelectorFactory(i, selector));
          aggregators.add(factorize);
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
      public void clear(boolean close)
      {
        for (BufferAggregator aggregator : aggregators) {
          aggregator.clear(close);
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

  private static class DimensionArrayColumnSelectorFactory extends ColumnSelectorFactory.ExprUnSupport
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
      final IndexedInts row = new IndexedInts.SingleValued()
      {
        @Override
        protected final int get()
        {
          return selector.getRow().get(index);
        }
      };

      return new DelegatedDimensionSelector(selector)
      {
        @Override
        public IndexedInts getRow()
        {
          return row;
        }
      };
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public Float get()
        {
          return Rows.parseFloat(selector.lookupName(selector.getRow().get(index)));
        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public Double get()
        {
          return Rows.parseDouble(selector.lookupName(selector.getRow().get(index)));
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public Long get()
        {
          return Rows.parseLong(selector.lookupName(selector.getRow().get(index)));
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public ValueDesc type()
        {
          return ValueDesc.STRING;
        }

        @Override
        public Object get()
        {
          return selector.lookupName(selector.getRow().get(index));
        }
      };
    }

    @Override
    public ValueDesc resolve(String columnName)
    {
      throw new UnsupportedOperationException("getColumnType");
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      throw new UnsupportedOperationException("getColumnNames");
    }
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DimensionArrayAggregatorFactory(delegate.getName(), delegate, limit);
  }

  @Override
  protected boolean isMergeable(AggregatorFactory other)
  {
    return super.isMergeable(other) &&
           Objects.equals(column, ((DimensionArrayAggregatorFactory) other).column) &&
           delegate.isMergeable(((DimensionArrayAggregatorFactory) other).delegate);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (!isMergeable(other)) {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
    final DimensionArrayAggregatorFactory array = (DimensionArrayAggregatorFactory) other;
    return new DimensionArrayAggregatorFactory(
        column,
        delegate.getMergingFactory(array.delegate),
        Math.max(limit, array.limit)
    );
  }
}
