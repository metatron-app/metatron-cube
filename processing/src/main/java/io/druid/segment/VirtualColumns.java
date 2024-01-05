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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.query.RowResolver;
import io.druid.query.RowSignature;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class VirtualColumns implements Iterable<VirtualColumn>
{
  public static VirtualColumns EMPTY = new VirtualColumns(ImmutableMap.of());

  public static void assertDimensionIndexed(RowResolver resolver, DimensionSpec dimension)
  {
    ValueDesc type = dimension.resolve(Suppliers.ofInstance(resolver));
    if (type.isMap()) {
      String[] descriptiveType = type.getDescription();
      if (descriptiveType == null) {
        throw new IAE("cannot resolve value type of map %s [%s]", type, dimension);
      }
      type = ValueDesc.of(descriptiveType[1]);
    }
    Preconditions.checkArgument(
        type.isDimension(), "group-by columns %s is not dimension indexed (%s)", dimension, type
    );
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

  public static Map<String, VirtualColumn> asMap(List<VirtualColumn> virtualColumns)
  {
    Map<String, VirtualColumn> map = Maps.newLinkedHashMap();
    for (VirtualColumn vc : virtualColumns) {
      if (map.put(vc.getOutputName(), vc.duplicate()) != null) {
        throw new IAE("overriding dimension [%s] by virtualColumn", vc.getOutputName());
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
      if (!mapping.containsKey(metric)) {
        implicit(mapping, metric, schema.resolve(metric, ValueDesc.UNKNOWN));
      }
    }
    return new VirtualColumns(mapping);
  }

  private static void implicit(Map<String, VirtualColumn> mapping, String metric, ValueDesc valueType)
  {
    if (valueType.isArray()) {
      mapping.put(metric, ArrayVirtualColumn.implicit(metric, valueType));   // implicit array vc
      implicit(mapping, metric, valueType.unwrapArray());
    } else if (valueType.isMap()) {
      mapping.put(metric, MapVirtualColumn.implict(metric, valueType));      // implicit map vc
    } else if (valueType.isStruct()) {
      if (!mapping.containsKey(metric)) {
        mapping.put(metric, StructVirtualColumn.implicit(metric, valueType));  // implicit struct vc
      }
      String[] description = TypeUtils.splitDescriptiveType(valueType);
      if (description == null) {
        return;
      }
      String prefix = metric + ".";
      for (int i = 1; i < description.length; i++) {
        int ix = description[i].indexOf(':');
        ValueDesc element = ValueDesc.of(description[i].substring(ix + 1));
        implicit(mapping, prefix + description[i].substring(0, ix), element);
      }
    } else if (valueType.isBitSet()) {
      mapping.put(metric, BitSetVirtualColumn.implicit(metric));  // implicit bitSet vc
    }
  }

  public static boolean needImplicitVC(ValueDesc valueType)
  {
    return valueType.isArray() || valueType.isMap() || valueType.isStruct() || valueType.isBitSet();
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
    return mimicDimensionSelector(ValueDesc.LONG, () -> selector.get());
  }

  public static DimensionSelector toDimensionSelector(ObjectColumnSelector selector, ExtractionFn extractionFn)
  {
    if (selector == null) {
      if (extractionFn == null) {
        return NullDimensionSelector.STRING_TYPE;
      } else {
        return new ColumnSelectors.SingleValuedDimensionSelector(extractionFn.apply(null));
      }
    } else if (extractionFn != null) {
      return new SingleValued(ValueDesc.STRING, () -> extractionFn.apply(selector.get()));
    }
    final ValueDesc type = selector.type();
    if (type.isPrimitive()) {
      return new SingleValued(type, selector);
    } else if (type.isMultiValued()) {
      return new MulitiValued(type.subElement(), selector);
    } else if (type.isArray()) {
      return new MulitiValued(type.unwrapArray(), selector);
    } else {
      return new SingleValued(ValueDesc.STRING, () -> Objects.toString(selector.get(), null));
    }
  }

  public static DimensionSelector mimicDimensionSelector(ValueDesc type, Supplier<Object> supplier)
  {
    return new SingleValued(type, supplier);
  }

  private static class SingleValued extends MimicDimension implements DimensionSelector.SingleValued
  {
    private final IndexedInts row;

    private SingleValued(ValueDesc type, Supplier<?> supplier)
    {
      super(type, supplier);
      this.row = IndexedInts.from(() -> register(supplier.get()));
    }

    @Override
    public IndexedInts getRow()
    {
      return row;
    }
  }

  private static class MulitiValued extends MimicDimension
  {
    private MulitiValued(ValueDesc type, Supplier<?> selector)
    {
      super(type, selector);
    }

    @Override
    public IndexedInts getRow()
    {
      final Object o = supplier.get();
      if (o instanceof List) {
        final List values = (List) o;
        return new IndexedInts()
        {
          @Override
          public int size()
          {
            return values.size();
          }

          @Override
          public int get(int index)
          {
            return register(values.get(index));
          }
        };
      }
      return new IndexedInts.SingleValued()
      {
        @Override
        public int get(int index)
        {
          return register(o);
        }
      };
    }
  }

  private static abstract class MimicDimension implements DimensionSelector.Mimic
  {
    final ValueDesc type;
    final Supplier<?> supplier;

    final Object2IntMap<Object> valToId = new Object2IntOpenHashMap<>();
    final List<Object> idToVal = Lists.newArrayList();

    private MimicDimension(ValueDesc type, Supplier<?> supplier)
    {
      this.type = type;
      this.supplier = supplier;
    }

    protected final int register(Object value)
    {
      return valToId.computeIntIfAbsent(value, v -> {idToVal.add(value);return idToVal.size() - 1;});
    }

    @Override
    public int getValueCardinality()
    {
      return idToVal.size();
    }

    @Override
    public Object lookupName(int id)
    {
      return idToVal.get(id);
    }

    @Override
    public ValueDesc type()
    {
      return type;
    }

    @Override
    public int lookupId(Object name)
    {
      return valToId.getOrDefault(name, -1);
    }

    @Override
    public Object get()
    {
      return supplier.get();
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
    final IndexedInts indexedInts = IndexedInts.from(index);

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
      public Object lookupName(int id)
      {
        return id >= 0 && id < cardinality ? idToVal.get(id) : null;
      }

      @Override
      public ValueDesc type()
      {
        return ValueDesc.STRING;
      }

      @Override
      public int lookupId(Object name)
      {
        return valToId.get(name);
      }
    };
  }

  // share indexing key of dimension selectors to metric selectors for proper pair-matching
  public static ColumnSelectorFactory forAggregators(Cursor factory, List<VirtualColumn> virtualColumns)
  {
    if (virtualColumns.isEmpty()) {
      return factory;
    }
    final Map<String, VirtualColumn> sources = Maps.newHashMap();
    final Map<String, ColumnSelectorFactory> targets = Maps.newHashMap();
    for (VirtualColumn column : virtualColumns) {
      if (column instanceof VirtualColumn.IndexProvider) {
        VirtualColumn.IndexProvider provider = (VirtualColumn.IndexProvider) column;
        if (provider.sourceColumn() != null) {
          Preconditions.checkArgument(sources.put(provider.sourceColumn(), provider) == null);
        }
        ColumnSelectorFactory overriden = provider.override(factory);
        for (String targetColumn : provider.targetColumns()) {
          Preconditions.checkArgument(targets.put(targetColumn, overriden) == null);
        }
      }
    }
    if (sources.isEmpty() && targets.isEmpty()) {
      return factory;
    }
    return new ColumnSelectorFactory.ExprSupport()
    {
      @Override
      public Iterable<String> getColumnNames()
      {
        return factory.getColumnNames();
      }

      @Override
      public ValueDesc resolve(String columnName)
      {
        return targets.getOrDefault(columnName, factory).resolve(columnName);
      }

      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimension)
      {
        VirtualColumn vc = sources.get(dimension.getDimension());
        if (vc != null) {
          return vc.asDimension(dimension, factory);
        }
        return factory.makeDimensionSelector(dimension);
      }

      @Override
      public FloatColumnSelector makeFloatColumnSelector(String columnName)
      {
        return targets.getOrDefault(columnName, factory).makeFloatColumnSelector(columnName);
      }

      @Override
      public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
      {
        return targets.getOrDefault(columnName, factory).makeDoubleColumnSelector(columnName);
      }

      @Override
      public LongColumnSelector makeLongColumnSelector(String columnName)
      {
        return targets.getOrDefault(columnName, factory).makeLongColumnSelector(columnName);
      }

      @Override
      public ObjectColumnSelector makeObjectColumnSelector(String columnName)
      {
        return targets.getOrDefault(columnName, factory).makeObjectColumnSelector(columnName);
      }

      @Override
      public ValueMatcher makePredicateMatcher(DimFilter filter)
      {
        List<String> dependents = Lists.newArrayList(Filters.getDependents(filter));
        if (dependents.isEmpty()) {
          return factory.makePredicateMatcher(filter);
        }
        ColumnSelectorFactory prev = targets.getOrDefault(dependents.get(0), factory);
        for (int i = 1; i < dependents.size(); i++) {
          ColumnSelectorFactory current = targets.getOrDefault(dependents.get(i), factory);
          if (prev != null && prev != current) {
            throw new IllegalArgumentException("cannot access two independent factories");
          }
          prev = current;
        }
        return prev.makePredicateMatcher(filter);
      }

      @Override
      public Map<String, String> getDescriptor(String columnName)
      {
        return factory.getDescriptor(columnName);
      }
    };
  }

  @Override
  public Iterator<VirtualColumn> iterator()
  {
    return virtualColumns.values().iterator();
  }

  private final Map<String, VirtualColumn> virtualColumns;

  private VirtualColumns(Map<String, VirtualColumn> virtualColumns)
  {
    this.virtualColumns = virtualColumns;
  }

  public VirtualColumn getVirtualColumn(String dimension)
  {
    VirtualColumn vc = virtualColumns.get(dimension);
    for (int i = dimension.lastIndexOf('.'); vc == null && i > 0; i = dimension.lastIndexOf('.', i - 1)) {
      vc = virtualColumns.get(dimension.substring(0, i));
    }
    return vc;
  }

  public void resolveIndexProvider(String dimension, Set<String> fields)
  {
    VirtualColumn vc = getVirtualColumn(dimension);
    if (vc instanceof VirtualColumn.IndexProvider.Rewritable) {
      List<String> metrics = Lists.newArrayList();
      for (String field : fields) {
        VirtualColumn target = getVirtualColumn(field);
        if (target == vc) {
          fields.remove(field);
          metrics.add(field);
        }
      }
      VirtualColumn.IndexProvider.Rewritable rewritable = (VirtualColumn.IndexProvider.Rewritable) vc;
      if (!metrics.isEmpty()) {
        virtualColumns.put(vc.getOutputName(), rewritable.withIndexer(dimension, metrics));
      }
      int ix = vc.getOutputName().lastIndexOf('.');
      if (ix > 0 && !fields.isEmpty()) {
        // todo nested array??
        resolveIndexProvider(vc.getOutputName().substring(0, ix), fields);
      }
    }
  }

  public Set<String> getVirtualColumnNames()
  {
    return ImmutableSet.copyOf(virtualColumns.keySet());
  }

  public static ColumnSelectorFactory wrap(
      VirtualColumn source,
      ColumnSelectorFactory factory,
      String dimension,
      Set<String> metrics
  )
  {
    return new VirtualColumnAsColumnSelectorFactory(source, factory, dimension, metrics);
  }

  private static class VirtualColumnAsColumnSelectorFactory extends ColumnSelectorFactories.Delegated
  {
    private final VirtualColumn virtualColumn;
    private final String dimensionColumn;
    private final Set<String> metricColumns;

    private VirtualColumnAsColumnSelectorFactory(
        VirtualColumn virtualColumn,
        ColumnSelectorFactory factory,
        String dimensionColumn,
        Set<String> metricColumns
    )
    {
      super(factory);
      this.virtualColumn = virtualColumn;
      this.dimensionColumn = dimensionColumn;
      this.metricColumns = metricColumns;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimension)
    {
      if (dimensionColumn.equals(dimension.getDimension())) {
        return virtualColumn.asDimension(dimension, delegate);
      }
      return super.makeDimensionSelector(dimension);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asFloatMetric(columnName, delegate);
      }
      return super.makeFloatColumnSelector(columnName);
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asDoubleMetric(columnName, delegate);
      }
      return super.makeDoubleColumnSelector(columnName);
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asLongMetric(columnName, delegate);
      }
      return super.makeLongColumnSelector(columnName);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asMetric(columnName, delegate);
      }
      return super.makeObjectColumnSelector(columnName);
    }

    @Override
    public ValueDesc resolve(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.resolveType(columnName, delegate);
      }
      return super.resolve(columnName);
    }
  }
}
