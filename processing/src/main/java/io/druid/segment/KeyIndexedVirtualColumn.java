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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.metamx.common.StringUtils;
import io.druid.query.QueryCacheHelper;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 */
public class KeyIndexedVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x02;

  private final String outputName;
  private final String keyDimension;
  private final DimFilter keyFilter;
  private final List<String> valueDimensions;
  private final List<String> valueMetrics;

  private final Set<String> valueColumns;
  private final KeyIndexedHolder indexer;

  @JsonCreator
  public KeyIndexedVirtualColumn(
      @JsonProperty("keyDimension") String keyDimension,
      @JsonProperty("valueDimensions") List<String> valueDimensions,
      @JsonProperty("valueMetrics") List<String> valueMetrics,
      @JsonProperty("keyFilter") DimFilter keyFilter,
      @JsonProperty("outputName") String outputName
  )
  {
    Preconditions.checkArgument(keyDimension != null, "key dimension should not be null");
    this.keyDimension = keyDimension;
    this.keyFilter = keyFilter;
    this.valueDimensions = valueDimensions == null ? ImmutableList.<String>of() : valueDimensions;
    this.valueMetrics = valueMetrics == null ? ImmutableList.<String>of() : valueMetrics;
    Preconditions.checkArgument(
        !this.valueDimensions.isEmpty() || !this.valueMetrics.isEmpty(),
        "target column should not be empty"
    );
    this.outputName = Preconditions.checkNotNull(outputName, "output name should not be null");
    this.valueColumns = Sets.newHashSet(Iterables.concat(this.valueDimensions, this.valueMetrics));
    this.indexer = new KeyIndexedHolder();
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    if (valueDimensions.contains(column)) {
      final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of(column));

      return new ObjectColumnSelector<String>()
      {
        private transient int version;
        private transient IndexedInts values;

        @Override
        public Class<String> classOfObject()
        {
          return String.class;
        }

        @Override
        public String get()
        {
          if (indexer.version != version) {
            values = selector.getRow();
            version = indexer.version;
          }
          final int index = indexer.index();
          return values == null || index < 0 || index >= values.size() ? null : selector.lookupName(values.get(index));
        }
      };
    }

    if (valueMetrics.contains(column)) {
      @SuppressWarnings("unchecked")
      final ObjectColumnSelector<List> selector = factory.makeObjectColumnSelector(column);
      if (selector == null) {
        return new ObjectColumnSelector()
        {
          @Override
          public Class classOfObject()
          {
            return Object.class;
          }

          @Override
          public Object get()
          {
            return null;
          }
        };
      }
      if (selector.classOfObject() != List.class) {
        throw new IllegalArgumentException("target column '" + column + "' should be array type");
      }

      return new ObjectColumnSelector<Object>()
      {
        private transient int version;
        private transient List values;

        @Override
        public Class<Object> classOfObject()
        {
          return Object.class;
        }

        @Override
        public Object get()
        {
          if (indexer.version != version) {
            values = selector.get();
            version = indexer.version;
          }
          final int index = indexer.index();
          return values == null || index < 0 || index >= values.size() ? null : values.get(index);
        }
      };
    }
    return factory.makeObjectColumnSelector(column);
  }

  @Override
  public FloatColumnSelector asFloatMetric(String column, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asFloat(asMetric(column, factory));
  }

  @Override
  public DoubleColumnSelector asDoubleMetric(String column, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asDouble(asMetric(column, factory));
  }

  @Override
  public LongColumnSelector asLongMetric(String column, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asLong(asMetric(column, factory));
  }

  @Override
  public DimensionSelector asDimension(final String dimension, final ColumnSelectorFactory factory)
  {
    if (!dimension.equals(outputName)) {
      throw new IllegalStateException("Only can be called as a group-by/top-N dimension");
    }
    final DimensionSelector selector = toFilteredSelector(factory);

    return new IndexProvidingSelector.Delegated(selector)
    {
      @Override
      public final IndexedInts getRow()
      {
        return indexer.indexed(super.getRow());
      }

      @Override
      public final ColumnSelectorFactory wrapFactory(final ColumnSelectorFactory factory)
      {
        return new VirtualColumns.VirtualColumnAsColumnSelectorFactory(
            KeyIndexedVirtualColumn.this, factory, outputName, valueColumns
        );
      }

      @Override
      public Set<String> targetColumns()
      {
        return valueColumns;
      }
    };
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new KeyIndexedVirtualColumn(keyDimension, valueDimensions, valueMetrics, keyFilter, outputName);
  }

  private DimensionSelector toFilteredSelector(ColumnSelectorFactory factory)
  {
    final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of(keyDimension));
    if (keyFilter == null) {
      return selector;
    }
    final IteratingIndexedInts iterator = new IteratingIndexedInts(selector);
    final ValueMatcher keyMatcher = Filters.toFilter(keyFilter).makeMatcher(
        new ColumnSelectorFactories.Delegated(factory)
        {
          @Override
          public ObjectColumnSelector makeObjectColumnSelector(String columnName)
          {
            if (!columnName.equals(outputName)) {
              throw new UnsupportedOperationException("cannot reference column '" + columnName + "' in current context");
            }
            return new ObjectColumnSelector<IndexedInts.WithLookup>()
            {
              @Override
              public Class<IndexedInts.WithLookup> classOfObject()
              {
                return IndexedInts.WithLookup.class;
              }

              @Override
              public IndexedInts.WithLookup get()
              {
                return iterator;
              }
            };
          }
        }
    );
    return new DelegatedDimensionSelector(selector)
    {
      @Override
      public IndexedInts getRow()
      {
        final IndexedInts indexed = iterator.next();
        final int limit = indexed.size();
        final int[] mapping = indexer.mapping(limit);

        int virtual = 0;
        for (; iterator.index < limit; iterator.index++) {
          if (keyMatcher.matches()) {
            mapping[virtual++] = iterator.index;
          }
        }
        if (virtual == 0) {
          return EmptyIndexedInts.EMPTY_INDEXED_INTS;
        }
        final int size = virtual;

        return new IndexedInts()
        {
          @Override
          public int size()
          {
            return size;
          }

          @Override
          public int get(int index)
          {
            return index < size && mapping[index] >= 0 ? indexed.get(mapping[index]) : -1;
          }

          @Override
          public void fill(int index, int[] toFill)
          {
            throw new UnsupportedOperationException("fill");
          }

          @Override
          public void close() throws IOException
          {
            indexed.close();
          }

          @Override
          public Iterator<Integer> iterator()
          {
            return new Iterator<Integer>()
            {
              private int index;

              @Override
              public boolean hasNext()
              {
                return index < size;
              }

              @Override
              public Integer next()
              {
                return indexed.get(mapping[index++]);
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException("remove");
              }
            };
          }
        };
      }
    };
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] key = StringUtils.toUtf8(keyDimension);
    byte[] valueDims = QueryCacheHelper.computeCacheBytes(valueDimensions);
    byte[] valueMets = QueryCacheHelper.computeCacheBytes(valueMetrics);
    byte[] keyFilters = keyFilter == null ? new byte[0] : keyFilter.getCacheKey();
    byte[] output = StringUtils.toUtf8(outputName);

    return ByteBuffer.allocate(5 + key.length + valueDims.length + valueMets.length + keyFilters.length + output.length)
                     .put(VC_TYPE_ID)
                     .put(key).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(valueDims).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(valueMets).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(keyFilters).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(output)
                     .array();
  }

  @JsonProperty
  public String getKeyDimension()
  {
    return keyDimension;
  }

  @JsonProperty
  public List<String> getValueDimensions()
  {
    return valueDimensions;
  }

  @JsonProperty
  public List<String> getValueMetrics()
  {
    return valueMetrics;
  }

  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @JsonProperty
  public DimFilter getKeyFilter()
  {
    return keyFilter;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KeyIndexedVirtualColumn)) {
      return false;
    }

    KeyIndexedVirtualColumn that = (KeyIndexedVirtualColumn) o;

    if (!keyDimension.equals(that.keyDimension)) {
      return false;
    }
    if (!valueDimensions.equals(that.valueDimensions)) {
      return false;
    }
    if (!valueMetrics.equals(that.valueMetrics)) {
      return false;
    }
    if (!outputName.equals(that.outputName)) {
      return false;
    }
    if (!Objects.equals(keyFilter, that.keyFilter)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = keyDimension.hashCode();
    result = 31 * result + valueDimensions.hashCode();
    result = 31 * result + valueMetrics.hashCode();
    result = 31 * result + outputName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "KeyIndexedVirtualColumn{" +
           "keyDimension='" + keyDimension + '\'' +
           ", keyFilter=" + keyFilter +
           ", valueDimensions=" + valueDimensions +
           ", valueMetrics=" + valueMetrics +
           ", outputName='" + outputName + '\'' +
           '}';
  }

  private static class IteratingIndexedInts implements IndexedInts.WithLookup {

    private int index;
    private IndexedInts indexedInts;
    private final DimensionSelector selector;

    private IteratingIndexedInts(DimensionSelector selector)
    {
      this.selector = selector;
    }

    private IndexedInts next()
    {
      index = 0;
      return indexedInts = selector.getRow();
    }

    @Override
    public int lookupId(String name)
    {
      return selector.lookupId(name);
    }

    @Override
    public String lookupName(int id)
    {
      return selector.lookupName(id);
    }

    @Override
    public int size()
    {
      return 1;
    }

    @Override
    public int get(int dummy)
    {
      return indexedInts.get(index);
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
  }

  private static class KeyIndexedHolder extends IndexProvidingSelector.IndexHolder
  {
    private volatile int[] mapping;

    @Override
    final int index()
    {
      return mapping == null ? index : mapping[index];
    }

    final int[] mapping(int size)
    {
      if (mapping == null || mapping.length < size) {
        mapping = new int[size];
      }
      mapping[0] = -1;
      return mapping;
    }
  }
}
