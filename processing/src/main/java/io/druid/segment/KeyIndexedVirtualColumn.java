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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.IndexProvidingSelector.IndexHolder;
import io.druid.segment.data.IndexedID;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *
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
        return ColumnSelectors.nullObjectSelector(ValueDesc.UNKNOWN);
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
      public IndexedInts getRow()
      {
        return indexer.indexed(super.getRow());
      }

      @Override
      public ColumnSelectorFactory wrapFactory(final ColumnSelectorFactory factory)
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
    DimensionSpec dimensionSpec = DimensionSpecs.of(keyDimension, extractionFn);
    DimensionSelector selector = factory.makeDimensionSelector(dimensionSpec);
    if (keyFilter == null || extractionFn != null || selector instanceof DimensionSelector.SingleValued) {
      return selector;
    }
    DimensionSelector[] selectors = new DimensionSelector[]{selector};
    rewrite(factory, Arrays.asList(dimensionSpec), selectors, Arrays.asList(keyDimension), keyFilter);
    return selectors[0];
  }

  public static void rewrite(
      ColumnSelectorFactory factory,
      List<DimensionSpec> dimensions,
      DimensionSelector[] selectors,
      List<String> mvDimensions,
      DimFilter filter
  )
  {
    final DimFilter rewritten = DimFilters.rewrite(
        filter, f -> GuavaUtils.containsAny(Filters.getDependents(f), mvDimensions) ? f : null
    );
    final Set<String> dependents = GuavaUtils.retain(Filters.getDependents(rewritten), mvDimensions);
    if (dependents.isEmpty()) {
      return;
    }
    final List<String> inputNames = DimensionSpecs.toInputNames(dimensions);
    final Map<String, MVIteratingSelector> hacked = Maps.newHashMap();
    for (String dependent : dependents) {
      final int index = inputNames.indexOf(dependent);
      if (index < 0 || selectors[index] instanceof DimensionSelector.SingleValued) {
        continue;
      }
      hacked.put(dependent, new MVIteratingSelector(index, selectors[index]));
    }
    final ValueMatcher keyMatcher = Filters.toFilter(rewritten, factory).makeMatcher(
        new ColumnSelectorFactories.Delegated(factory)
        {
          @Override
          public ObjectColumnSelector makeObjectColumnSelector(String columnName)
          {
            return hacked.containsKey(columnName) ? hacked.get(columnName) : super.makeObjectColumnSelector(columnName);
          }

          @Override
          public ValueDesc resolve(String columnName)
          {
            return hacked.containsKey(columnName) ? INDEXED_TYPE : super.resolve(columnName);
          }
        }
    );
    for (ObjectColumnSelector selector : hacked.values()) {
      MVIteratingSelector mv = (MVIteratingSelector) selector;
      selectors[mv.source] = new DelegatedDimensionSelector(selectors[mv.source])
      {
        @Override
        public IndexedInts getRow() {return mv.rewrite(keyMatcher);}
      };
    }
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

  private static class MVIterator implements IndexedID
  {
    private int index;
    private IndexedInts indexedInts;
    private final DimensionSelector selector;

    private MVIterator(DimensionSelector selector)
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
    public Object lookupName(int id)
    {
      return selector.lookupName(id);
    }

    @Override
    public ValueType elementType()
    {
      return ValueType.STRING;
    }
  }

  private static final ValueDesc INDEXED_TYPE = ValueDesc.ofIndexedId(ValueType.STRING);

  private static class MVIteratingSelector extends ObjectColumnSelector.Typed<IndexedID>
  {
    private final int source;
    private final MVIterator iterator;

    private MVIteratingSelector(int source, DimensionSelector selector)
    {
      super(INDEXED_TYPE);
      this.source = source;
      this.iterator = new MVIterator(selector);
    }

    @Override
    public IndexedID get()
    {
      return iterator;
    }

    private IndexedInts rewrite(ValueMatcher matcher)
    {
      final IndexedInts indexed = iterator.next();
      final int[] rewritten = new int[indexed.size()];
      for (; iterator.index < rewritten.length; iterator.index++) {
        rewritten[iterator.index] = matcher.matches() ? indexed.get(iterator.index) : -1;
      }
      return IndexedInts.from(rewritten);
    }
  }
}
