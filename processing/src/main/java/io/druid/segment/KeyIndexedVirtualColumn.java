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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.segment.data.IndexedInts;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 *
 */
public class KeyIndexedVirtualColumn implements VirtualColumn.IndexProvider
{
  private static final byte VC_TYPE_ID = 0x02;

  private final String outputName;
  private final String keyDimension;
  private final DimFilter keyFilter;
  private final List<String> valueDimensions;
  private final List<String> valueMetrics;

  private final Set<String> valueColumns;
  private final IndexHolder indexer;

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
    this.indexer = new IndexHolder();
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    if (valueDimensions.contains(column)) {
      return ValueDesc.STRING;
    }
    if (valueMetrics.contains(column)) {
      return types.resolve(column, ValueDesc.UNKNOWN).subElement(ValueDesc.UNKNOWN);
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

      return new ObjectColumnSelector.Typed(selector.type().unwrapDimension())
      {
        private transient int version;
        private transient IndexedInts values;

        @Override
        public Object get()
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
        return ColumnSelectors.NULL_UNKNOWN;
      }
      ValueDesc type = selector.type();
      Preconditions.checkArgument(type.hasSubElement(), "cannot resolve element type");
      final ValueDesc elementType = type.subElement();

      return new ObjectColumnSelector.Typed<Object>(elementType)
      {
        private transient int version;
        private transient List values;

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
  public DimensionSelector asDimension(DimensionSpec dimension, ColumnSelectorFactory factory)
  {
    if (!outputName.equals(dimension.getDimension())) {
      throw new IllegalStateException("Only can be called as a group-by/top-N dimension");
    }
    return wrap(toFilteredSelector(factory, dimension.getExtractionFn()));
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new KeyIndexedVirtualColumn(keyDimension, valueDimensions, valueMetrics, keyFilter, outputName);
  }

  private DimensionSelector toFilteredSelector(final ColumnSelectorFactory factory, final ExtractionFn extractionFn)
  {
    DimensionSpec dimensionSpec = DimensionSpecs.of(keyDimension, extractionFn);
    DimensionSelector selector = factory.makeDimensionSelector(dimensionSpec);
    if (keyFilter == null || extractionFn != null || selector instanceof DimensionSelector.SingleValued) {
      return selector;
    }
    MVIteratingSelector wrapper = MVIteratingSelector.wrap(selector);
    MVIteratingSelector.rewrite(factory, ImmutableMap.of(keyDimension, wrapper), keyFilter);
    return wrapper;
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

  @Override
  public String sourceColumn()
  {
    return keyDimension;
  }

  @Override
  public Set<String> targetColumns()
  {
    return valueColumns;
  }

  public DimensionSelector wrap(DimensionSelector selector)
  {
    return wrap(selector, indexer);
  }

  public static DimensionSelector wrap(DimensionSelector selector, IndexHolder indexer)
  {
    return new DimensionSelector.Delegated(selector)
    {
      @Override
      public IndexedInts getRow()
      {
        return indexer.indexed(super.getRow());
      }
    };
  }

  public Function<IndexedInts, IndexedInts> indexer()
  {
    return indexer::indexed;
  }

  // single threaded by group-by engine.. no need to be volatile
  static class IndexHolder
  {
    int version;
    int index;

    int index()
    {
      return index;
    }

    IndexedInts indexed(final IndexedInts delegate)
    {
      version++;
      return new IndexedInts()
      {
        @Override
        public int size()
        {
          return delegate.size();
        }

        @Override
        public int get(int index)
        {
          IndexHolder.this.index = index;
          return delegate.get(index);
        }

        @Override
        public void close() throws IOException
        {
          delegate.close();
        }
      };
    }
  }
}
