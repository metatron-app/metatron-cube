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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.IndexedID;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;

import java.io.IOException;
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
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    if (valueDimensions.contains(column)) {
      return ValueDesc.STRING;
    }
    if (valueMetrics.contains(column)) {
      return types.resolve(column).subElement(ValueDesc.UNKNOWN);
    }
    if (outputName.equals(column)) {
      return types.resolve(keyDimension);
    }
    return types.resolve(column);
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    if (valueDimensions.contains(column)) {
      final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of(column));

      return new ObjectColumnSelector()
      {
        private transient int version;
        private transient IndexedInts values;

        @Override
        public ValueDesc type()
        {
          return ValueDesc.assertPrimitive(selector.type());
        }

        @Override
        public Comparable get()
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
        return ColumnSelectors.nullObjectSelector(ValueDesc.UNKNOWN);
      }
      ValueDesc type = selector.type();
      Preconditions.checkArgument(type.hasSubElement(), "cannot resolve element type");
      final ValueDesc elementType = type.subElement();

      return new ObjectColumnSelector<Object>()
      {
        private transient int version;
        private transient List values;

        @Override
        public ValueDesc type()
        {
          return elementType;
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
  public DimensionSelector asDimension(
      final String dimension,
      final ExtractionFn extractionFn,
      final ColumnSelectorFactory factory
  )
  {
    if (!dimension.equals(outputName)) {
      throw new IllegalStateException("Only can be called as a group-by/top-N dimension");
    }
    final DimensionSelector selector = toFilteredSelector(factory, extractionFn);

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

  private DimensionSelector toFilteredSelector(final ColumnSelectorFactory factory, final ExtractionFn extractionFn)
  {
    final DimensionSpec dimensionSpec = DimensionSpecs.of(keyDimension, extractionFn);
    final DimensionSelector selector = factory.makeDimensionSelector(dimensionSpec);
    if (keyFilter == null) {
      return selector;
    }
    final IteratingIndexedInts iterator = new IteratingIndexedInts(selector);
    final ValueMatcher keyMatcher = Filters.toFilter(keyFilter, factory).makeMatcher(
        new ColumnSelectorFactories.NotSupports()
        {
          @Override
          public ObjectColumnSelector makeObjectColumnSelector(String columnName)
          {
            Preconditions.checkArgument(
                columnName.equals(outputName), "cannot reference column '%s' in current context", columnName
            );
            return new ObjectColumnSelector<IndexedID>()
            {
              @Override
              public ValueDesc type()
              {
                return ValueDesc.ofIndexedId(ValueType.STRING);
              }

              @Override
              public IndexedID get()
              {
                return iterator;
              }
            };
          }

          @Override
          public ValueDesc resolve(String columnName)
          {
            Preconditions.checkArgument(
                columnName.equals(outputName), "cannot reference column '%s' in current context", columnName
            );
            return ValueDesc.ofIndexedId(ValueType.STRING);
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
          public void close() throws IOException
          {
            indexed.close();
          }
        };
      }
    };
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(VC_TYPE_ID)
                  .append(keyDimension).sp()
                  .append(valueDimensions).sp()
                  .append(valueMetrics).sp()
                  .append(keyFilter).sp()
                  .append(outputName);
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

  @Override
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

  private static class IteratingIndexedInts implements IndexedID
  {
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
    public int get()
    {
      return indexedInts.get(index);
    }

    @Override
    public int lookupId(String name)
    {
      return selector.lookupId(name);
    }

    @Override
    public Comparable lookupName(int id)
    {
      return selector.lookupName(id);
    }

    @Override
    public ValueType elementType()
    {
      return ValueType.STRING;
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
