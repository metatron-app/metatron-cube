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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.query.RowResolver;
import io.druid.query.RowSignature;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class VirtualColumns implements Iterable<VirtualColumn>
{
  public static void assertDimensionIndexed(RowResolver resolver, DimensionSpec dimension)
  {
    ValueDesc type = dimension.resolve(resolver);
    if (type.isMap()) {
      String[] descriptiveType = TypeUtils.splitDescriptiveType(type.typeName());
      if (descriptiveType == null) {
        throw new ISE("cannot resolve value type of map %s [%s]", type, dimension);
      }
      type = ValueDesc.of(descriptiveType[1]);
    }
    Preconditions.checkArgument(type.isDimension(), "group-by columns " + dimension + " is not dimension indexed (" + type + ")");
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

  public static VirtualColumns valueOf(List<VirtualColumn> virtualColumns, RowSignature schema)
  {
    Map<String, VirtualColumn> mapping = Maps.newLinkedHashMap();
    for (VirtualColumn vc : virtualColumns) {
      if (mapping.put(vc.getOutputName(), vc.duplicate()) != null) {
        throw new IAE("conflicting output names %s in virtualColumns", vc.getOutputName());
      }
    }
    for (String metric : schema.getMetricNames()) {
      if (mapping.containsKey(metric)) {
        continue;
      }
      ValueDesc valueType = schema.resolve(metric, ValueDesc.UNKNOWN);
      if (valueType.isArray()) {
        mapping.put(metric, ArrayVirtualColumn.implicit(metric));  // implicit array vc
      } else if (valueType.isStruct()) {
        mapping.put(metric, StructVirtualColumn.implicit(metric));  // implicit struct vc
      }
    }
    return new VirtualColumns(mapping);
  }

  public static List<VirtualColumn> override(List<VirtualColumn> original, List<VirtualColumn> overriding)
  {
    if (GuavaUtils.isNullOrEmpty(overriding)) {
      return original;
    }
    Map<String, VirtualColumn> vcs = VirtualColumns.asMap(original);
    for (VirtualColumn vc : overriding) {
      vcs.put(vc.getOutputName(), vc);    // override
    }
    return Lists.newArrayList(vcs.values());
  }

  public static DimensionSelector toDimensionSelector(final LongColumnSelector selector)
  {
    final Supplier<Long> supplier = new Supplier<Long>()
    {
      @Override
      public Long get()
      {
        return selector.get();
      }
    };
    return new MimicDimension(ValueDesc.LONG, supplier);
  }

  public static DimensionSelector toDimensionSelector(
      final ObjectColumnSelector selector,
      final ExtractionFn extractionFn
  )
  {
    if (selector == null) {
      if (extractionFn == null) {
        return NullDimensionSelector.STRING_TYPE;
      }
      return new ColumnSelectors.SingleValuedDimensionSelector(extractionFn.apply(null));
    }

    final ValueDesc type;
    final ValueDesc valueDesc = selector.type();
    if (extractionFn != null || !ValueDesc.isPrimitive(valueDesc)) {
      type = ValueDesc.STRING;
    } else {
      type = valueDesc;
    }
    final Supplier<Comparable> supplier = new Supplier<Comparable>()
    {
      @Override
      public Comparable get()
      {
        final Object selected = selector.get();
        final Comparable value;
        if (extractionFn != null) {
          value = extractionFn.apply(selected);
        } else if (!type.isPrimitive()) {
          value = Objects.toString(selected, null);
        } else {
          value = (Comparable) selected;
        }
        return value;
      }
    };
    return new MimicDimension(type, supplier);
  }

  private static class MimicDimension implements DimensionSelector
  {
    private final ValueDesc type;
    private final Supplier<? extends Comparable> supplier;

    private int counter;
    private final Map<Comparable, Integer> valToId = Maps.newHashMap();
    private final List<Comparable> idToVal = Lists.newArrayList();

    private MimicDimension(ValueDesc type, Supplier<? extends Comparable> supplier) {
      this.type = type;
      this.supplier = supplier;
    }

    @Override
    public IndexedInts getRow()
    {
      final Comparable value = supplier.get();
      Integer index = valToId.get(value);
      if (index == null) {
        valToId.put(value, index = counter++);
        idToVal.add(value);
      }
      final int result = index;
      return new IndexedInts.SingleValued()
      {
        @Override
        protected final int get()
        {
          return result;
        }
      };
    }

    @Override
    public int getValueCardinality()
    {
      return -1;
    }

    @Override
    public Comparable lookupName(int id)
    {
      return idToVal.get(id);
    }

    @Override
    public ValueDesc type()
    {
      return type;
    }

    @Override
    public int lookupId(Comparable name)
    {
      return valToId.get(name);
    }
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
      public Comparable lookupName(int id)
      {
        return id >= 0 && id < cardinality ? idToVal.get(id) : null;
      }

      @Override
      public ValueDesc type()
      {
        return ValueDesc.STRING;
      }

      @Override
      public int lookupId(Comparable name)
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
      public ValueMatcher makePredicateMatcher(DimFilter filter)
      {
        List<String> dependents = Lists.newArrayList(Filters.getDependents(filter));
        if (dependents.isEmpty()) {
          return factory.makePredicateMatcher(filter);
        }
        ColumnSelectorFactory prev = delegate.getOrDefault(dependents.get(0), factory);
        for (int i = 1; i < dependents.size(); i++) {
          ColumnSelectorFactory current = delegate.getOrDefault(dependents.get(i), factory);
          if (prev != null && prev != current) {
            throw new IllegalArgumentException("cannot access two independent factories");
          }
          prev = current;
        }
        return prev.makePredicateMatcher(filter);
      }

      @Override
      public ValueDesc resolve(String columnName)
      {
        ColumnSelectorFactory wrapped = delegate.get(columnName);
        if (wrapped != null) {
          return wrapped.resolve(columnName);
        }
        return factory.resolve(columnName);
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
        return virtualColumn.asDimension(dimensionSpec.getDimension(), dimensionSpec.getExtractionFn(), factory);
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
    public ValueDesc resolve(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.resolveType(columnName, factory);
      }
      return factory.resolve(columnName);
    }
  }

  private final Map<String, VirtualColumn> virtualColumns;

  private VirtualColumns(Map<String, VirtualColumn> virtualColumns)
  {
    this.virtualColumns = virtualColumns;
  }

  public VirtualColumn getVirtualColumn(String dimension)
  {
    VirtualColumn vc = virtualColumns.get(dimension);
    for (int i = dimension.length(); vc == null && i > 0; ) {
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
