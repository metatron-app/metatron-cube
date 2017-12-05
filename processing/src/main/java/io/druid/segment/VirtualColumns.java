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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.data.ValueDesc;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class VirtualColumns implements Iterable<VirtualColumn>
{
  public static void checkDimensionIndexed(List<VirtualColumn> virtualColumns, String dimension)
  {
    if (virtualColumns != null && !virtualColumns.isEmpty()) {
      for (VirtualColumn vc : virtualColumns) {
        if (vc.getOutputName().equals(dimension)) {
          Preconditions.checkArgument(vc.isIndexed(dimension), "cannot reference virtual column in this context");
        }
      }
    }
  }

  public static Set<String> getVirtualColumnNames(List<VirtualColumn> virtualColumns)
  {
    if (virtualColumns != null && !virtualColumns.isEmpty()) {
      Set<String> vcNames = Sets.newHashSet();
      for (VirtualColumn vc : virtualColumns) {
        vcNames.add(vc.getOutputName());
      }
      return vcNames;
    }
    return ImmutableSet.of();
  }

  public static VirtualColumns empty()
  {
    return new VirtualColumns(Maps.<String, VirtualColumn>newHashMap());
  }

  public static Map<String, VirtualColumn> asMap(List<VirtualColumn> virtualColumns)
  {
    Map<String, VirtualColumn> map = Maps.newLinkedHashMap();
    for (VirtualColumn vc : virtualColumns) {
      if (map.put(vc.getOutputName(), vc.duplicate()) != null) {
        throw new IllegalArgumentException("duplicated columns in virtualColumns");
      }
    }
    return map;
  }

  public static VirtualColumns valueOf(VirtualColumn virtualColumn)
  {
    return virtualColumn == null ? empty() : valueOf(Arrays.asList(virtualColumn));
  }

  public static VirtualColumns valueOf(List<VirtualColumn> virtualColumns)
  {
    if (virtualColumns == null || virtualColumns.isEmpty()) {
      return empty();
    }
    return new VirtualColumns(asMap(virtualColumns));
  }

  public static DimensionSelector toDimensionSelector(final ObjectColumnSelector selector)
  {
    if (selector == null) {
      return new NullDimensionSelector();
    }

    return new DimensionSelector()
    {
      private int counter;
      private final Map<String, Integer> valToId = Maps.newHashMap();
      private final List<String> idToVal = Lists.newArrayList();

      @Override
      public IndexedInts getRow()
      {
        Object selected = selector.get();
        String value = selected == null ? null : String.valueOf(selected);
        Integer index = valToId.get(value);
        if (index == null) {
          valToId.put(value, index = counter++);
          idToVal.add(value);
        }
        final int result = index;
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
            return result;
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
            return Iterators.singletonIterator(result);
          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public String lookupName(int id)
      {
        return idToVal.get(id);
      }

      @Override
      public int lookupId(String name)
      {
        return valToId.get(name);
      }
    };
  }

  public static DimensionSelector toFixedDimensionSelector(List<String> values)
  {
    final int[] index = new int[values.size()];
    final Map<String, Integer> valToId = Maps.newHashMap();
    final List<String> idToVal = Lists.newArrayList();
    for (int i = 0; i < index.length; i++) {
      String value = values.get(i);
      Integer id = valToId.get(value);
      if (id == null) {
        valToId.put(value, id = idToVal.size());
        idToVal.add(value);
      }
      index[i] = id;
    }
    final int cardinality = idToVal.size();
    final IndexedInts indexedInts = new ArrayBasedIndexedInts(index);

    return new DimensionSelector()
    {

      @Override
      public IndexedInts getRow()
      {
        return indexedInts;
      }

      @Override
      public int getValueCardinality()
      {
        return cardinality;
      }

      @Override
      public String lookupName(int id)
      {
        return id >= 0 && id < cardinality ? idToVal.get(id) : null;
      }

      @Override
      public int lookupId(String name)
      {
        return valToId.get(name);
      }
    };
  }

  public static ColumnSelectorFactory wrap(List<IndexProvidingSelector> selectors, final ColumnSelectorFactory factory)
  {
    if (selectors.isEmpty()) {
      return factory;
    }
    final Map<String, ColumnSelectorFactory> delegate = Maps.newHashMap();
    for (IndexProvidingSelector selector : selectors) {
      ColumnSelectorFactory wrapped = selector.wrapFactory(factory);
      for (String targetColumn : selector.targetColumns()) {
        Preconditions.checkArgument(delegate.put(targetColumn, wrapped) == null);
      }
    }
    return new ColumnSelectorFactory.ExprSupport()
    {
      @Override
      public Iterable<String> getColumnNames()
      {
        return factory.getColumnNames();
      }

      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        return factory.makeDimensionSelector(dimensionSpec);
      }

      @Override
      public FloatColumnSelector makeFloatColumnSelector(String columnName)
      {
        ColumnSelectorFactory wrapped = delegate.get(columnName);
        if (wrapped != null) {
          return wrapped.makeFloatColumnSelector(columnName);
        }
        return factory.makeFloatColumnSelector(columnName);
      }

      @Override
      public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
      {
        ColumnSelectorFactory wrapped = delegate.get(columnName);
        if (wrapped != null) {
          return wrapped.makeDoubleColumnSelector(columnName);
        }
        return factory.makeDoubleColumnSelector(columnName);
      }

      @Override
      public LongColumnSelector makeLongColumnSelector(String columnName)
      {
        ColumnSelectorFactory wrapped = delegate.get(columnName);
        if (wrapped != null) {
          return wrapped.makeLongColumnSelector(columnName);
        }
        return factory.makeLongColumnSelector(columnName);
      }

      @Override
      public ObjectColumnSelector makeObjectColumnSelector(String columnName)
      {
        ColumnSelectorFactory wrapped = delegate.get(columnName);
        if (wrapped != null) {
          return wrapped.makeObjectColumnSelector(columnName);
        }
        return factory.makeObjectColumnSelector(columnName);
      }

      @Override
      public ValueMatcher makeAuxiliaryMatcher(DimFilter filter)
      {
        Set<String> dependents = Filters.getDependents(filter);
        if (dependents.size() != 1) {
          return null;
        }
        String columnName = Iterables.getOnlyElement(dependents);
        ColumnSelectorFactory wrapped = delegate.get(columnName);
        if (wrapped != null) {
          return wrapped.makeAuxiliaryMatcher(filter);
        }
        return factory.makeAuxiliaryMatcher(filter);
      }

      @Override
      public ValueDesc getColumnType(String columnName)
      {
        ColumnSelectorFactory wrapped = delegate.get(columnName);
        if (wrapped != null) {
          return wrapped.getColumnType(columnName);
        }
        return factory.getColumnType(columnName);
      }
    };
  }

  @Override
  public Iterator<VirtualColumn> iterator()
  {
    return virtualColumns.values().iterator();
  }

  public static class VirtualColumnAsColumnSelectorFactory extends ColumnSelectorFactory.ExprUnSupport
  {
    private final VirtualColumn virtualColumn;
    private final ColumnSelectorFactory factory;
    private final String dimensionColumn;
    private final Set<String> metricColumns;

    public VirtualColumnAsColumnSelectorFactory(
        VirtualColumn virtualColumn,
        ColumnSelectorFactory factory,
        String dimensionColumn,
        Set<String> metricColumns
    )
    {
      this.virtualColumn = virtualColumn;
      this.factory = factory;
      this.dimensionColumn = dimensionColumn;
      this.metricColumns = metricColumns;
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return factory.getColumnNames();
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      if (dimensionColumn.equals(dimensionSpec.getDimension())) {
        return virtualColumn.asDimension(dimensionSpec.getDimension(), factory);
      }
      return factory.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asFloatMetric(columnName, factory);
      }
      return factory.makeFloatColumnSelector(columnName);
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asDoubleMetric(columnName, factory);
      }
      return factory.makeDoubleColumnSelector(columnName);
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asLongMetric(columnName, factory);
      }
      return factory.makeLongColumnSelector(columnName);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asMetric(columnName, factory);
      }
      return factory.makeObjectColumnSelector(columnName);
    }

    @Override
    public ValueDesc getColumnType(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        throw new UnsupportedOperationException("getColumnType");
      }
      return factory.getColumnType(columnName);
    }
  }

  private final Map<String, VirtualColumn> virtualColumns;

  private VirtualColumns(Map<String, VirtualColumn> virtualColumns)
  {
    this.virtualColumns = virtualColumns;
  }

  public void addVirtualColumn(VirtualColumn vc)
  {
    virtualColumns.put(vc.getOutputName(), vc);
  }

  public VirtualColumn getVirtualColumn(String dimension)
  {
    VirtualColumn vc = virtualColumns.get(dimension);
    for (int i = dimension.length(); vc == null && i > 0;) {
      int index = dimension.lastIndexOf('.', i - 1);
      if (index > 0) {
        vc = virtualColumns.get(dimension.substring(0, index));
      }
      i = index;
    }
    return vc;
  }

  public Set<String> getVirtualColumnNames()
  {
    return ImmutableSet.copyOf(virtualColumns.keySet());
  }
}
