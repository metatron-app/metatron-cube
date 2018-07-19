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

package io.druid.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Expression;
import io.druid.math.expr.Expression.RelationExpression;
import io.druid.math.expr.Expressions;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BitmapType;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.select.Schema;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.data.IndexedID;
import io.druid.segment.filter.Filters;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class RowResolver implements TypeResolver, Function<String, ValueDesc>
{
  public static Supplier<RowResolver> supplier(final List<Segment> segments, final Query query)
  {
    return Suppliers.memoize(
        new Supplier<RowResolver>()
        {
          @Override
          public RowResolver get()
          {
            return of(segments, BaseQuery.getVirtualColumns(query));
          }
        }
    );
  }

  public static Supplier<RowResolver> supplier(final Segment segment, final Query query)
  {
    return Suppliers.memoize(
        new Supplier<RowResolver>()
        {
          @Override
          public RowResolver get()
          {
            return RowResolver.of(segment, BaseQuery.getVirtualColumns(query));
          }
        }
    );
  }

  public static RowResolver of(List<Segment> segments, VirtualColumns virtualColumns)
  {
    Preconditions.checkArgument(!segments.isEmpty());
    RowResolver resolver = of(segments.get(0), virtualColumns);
    for (int i = 1; i < segments.size(); i++) {
      RowResolver other = of(segments.get(i), virtualColumns);
      if (!resolver.columnTypes.equals(other.columnTypes)) {
        MapDifference<String, ValueDesc> difference = Maps.difference(resolver.columnTypes, other.columnTypes);
        if (difference.areEqual()) {
          continue;
        }
        for (Map.Entry<String, ValueDesc> entry : difference.entriesOnlyOnRight().entrySet()) {
          String columnName = entry.getKey();
          resolver.columnTypes.put(columnName, entry.getValue());
          resolver.columnCapabilities.put(columnName, other.getColumnCapabilities(columnName));
          if (!resolver.dimensionNames.contains(columnName) && other.isDimension(columnName)) {
            resolver.dimensionNames.add(columnName);
          } else if (!resolver.metricNames.contains(columnName) && other.isMetric(columnName)) {
            resolver.metricNames.add(columnName);
            resolver.aggregators.put(columnName, other.aggregators.get(columnName));
          }
        }
        for (Map.Entry<String, ValueDifference<ValueDesc>> entry : difference.entriesDiffering().entrySet()) {
          ValueDifference<ValueDesc> value = entry.getValue();
          ValueDesc left = value.leftValue();
          ValueDesc right = value.rightValue();
          ValueDesc resolved = ValueDesc.UNKNOWN;
          if (left.isNumeric() && right.isNumeric()) {
            resolved = ValueDesc.DOUBLE;
          }
          resolver.columnTypes.put(entry.getKey(), resolved);
        }
      }
      MapDifference<String, Map<String, String>> difference = Maps.difference(
          resolver.columnDescriptors,
          other.columnDescriptors
      );
    }
    virtualColumns.addImplicitVCs(resolver);
    return resolver;
  }

  public static RowResolver of(Segment segment, VirtualColumns virtualColumns)
  {
    RowResolver resolver = of(segment.asQueryableIndex(false), virtualColumns);
    if (resolver == null) {
      resolver = of(segment.asStorageAdapter(false), virtualColumns);
    }
    return resolver;
  }

  public static RowResolver of(QueryableIndex index, VirtualColumns virtualColumns)
  {
    return index == null ? null : new RowResolver(index, virtualColumns);
  }

  public static RowResolver of(StorageAdapter adapter, VirtualColumns virtualColumns)
  {
    return new RowResolver(adapter, virtualColumns);
  }

  public static RowResolver of(Schema schema, VirtualColumns virtualColumns)
  {
    return new RowResolver(schema, virtualColumns);
  }

  public static RowResolver outOf(Query.DimensionSupport<?> query)
  {
    Preconditions.checkArgument(!(query.getDataSource() instanceof ViewDataSource), "fix this");
    List<AggregatorFactory> aggregatorFactories = Lists.newArrayList();
    if (query instanceof Query.AggregationsSupport) {
      aggregatorFactories = ((Query.AggregationsSupport<?>)query).getAggregatorSpecs();
    }
    VirtualColumns vcs = VirtualColumns.valueOf(query.getVirtualColumns());
    return new RowResolver(query.getDimensions(), aggregatorFactories, vcs);
  }

  public static Class<?> toClass(ValueDesc valueDesc)
  {
    switch (valueDesc.type()) {
      case STRING:
        return String.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case LONG:
        return Long.class;
      case DATETIME:
        return DateTime.class;
    }
    String typeName = valueDesc.typeName();
    switch (typeName.toLowerCase()) {
      case ValueDesc.MAP_TYPE:
        return Map.class;
      case ValueDesc.LIST_TYPE:
        return List.class;
      case ValueDesc.UNKNOWN_TYPE:
        return Object.class;
    }

    if (typeName.startsWith(ValueDesc.INDEXED_ID_PREFIX)) {
      return IndexedID.class;
    }

    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
    if (serde != null) {
      return serde.getObjectStrategy().getClazz();
    }
    return Object.class;
  }

  public static ValueDesc toValueType(Object obj)
  {
    return obj == null ? ValueDesc.UNKNOWN : toValueType(obj.getClass(), obj);
  }

  private static ValueDesc toValueType(Class clazz, Object object)
  {
    if (clazz == Object.class) {
      return ValueDesc.UNKNOWN;
    } else if (clazz == String.class) {
      return ValueDesc.STRING;
    } else if (clazz == Float.class || clazz == Float.TYPE) {
      return ValueDesc.FLOAT;
    } else if (clazz == Double.class || clazz == Double.TYPE) {
      return ValueDesc.DOUBLE;
    } else if (clazz == Long.class || clazz == Long.TYPE) {
      return ValueDesc.LONG;
    } else if (clazz == DateTime.class) {
      return ValueDesc.DATETIME;
    } else if (clazz == BigDecimal.class) {
      return ValueDesc.DECIMAL;
    } else if (Map.class.isAssignableFrom(clazz)) {
      return ValueDesc.MAP;
    } else if (List.class.isAssignableFrom(clazz)) {
      return ValueDesc.LIST;
    } else if (IndexedID.class.isAssignableFrom(clazz)) {
      IndexedID lookup = (IndexedID)object;
      return lookup == null ? ValueDesc.INDEXED_ID : ValueDesc.ofIndexedId(lookup.elementType());
    } else if (clazz.isArray()) {
      return ValueDesc.ofArray(toValueType(clazz.getComponentType(), null));
    }
    // cannot make multi-valued type from class

    String typeName = ComplexMetrics.getTypeNameForClass(clazz);
    if (typeName != null) {
      return ValueDesc.of(typeName);
    }
    return ValueDesc.UNKNOWN;
  }

  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final VirtualColumns virtualColumns;
  private final Map<String, AggregatorFactory> aggregators;

  private final Map<String, ValueDesc> columnTypes = Maps.newHashMap();
  private final Map<String, ColumnCapabilities> columnCapabilities = Maps.newHashMap();
  private final Map<String, Map<String, String>> columnDescriptors = Maps.newHashMap();
  private final Map<String, Pair<VirtualColumn, ValueDesc>> virtualColumnTypes = Maps.newConcurrentMap();

  private RowResolver(StorageAdapter adapter, VirtualColumns virtualColumns)
  {
    this.dimensionNames = Lists.newArrayList(adapter.getAvailableDimensions());
    this.metricNames = Lists.newArrayList(adapter.getAvailableMetrics());
    this.virtualColumns = virtualColumns;
    this.aggregators = AggregatorFactory.getAggregatorsFromMeta(adapter.getMetadata());

    for (String dimension : adapter.getAvailableDimensions()) {
      columnTypes.put(dimension, adapter.getColumnType(dimension));
      columnCapabilities.put(dimension, adapter.getColumnCapabilities(dimension));
    }
    for (String metric : adapter.getAvailableMetrics()) {
      columnTypes.put(metric, adapter.getColumnType(metric));
      columnCapabilities.put(metric, adapter.getColumnCapabilities(metric));
    }
    columnTypes.put(Column.TIME_COLUMN_NAME, ValueDesc.LONG);
    columnCapabilities.put(Column.TIME_COLUMN_NAME, ColumnCapabilitiesImpl.of(ValueType.LONG));
    virtualColumns.addImplicitVCs(this);
  }

  private RowResolver(QueryableIndex index, VirtualColumns virtualColumns)
  {
    this.dimensionNames = Lists.newArrayList(index.getAvailableDimensions());
    this.metricNames = Lists.newArrayList(index.getAvailableMetrics());
    this.virtualColumns = virtualColumns;
    this.aggregators = AggregatorFactory.getAggregatorsFromMeta(index.getMetadata());

    for (String columnName : index.getColumnNames()) {
      Column column = index.getColumn(columnName);
      columnTypes.put(columnName, index.getColumnType(columnName));
      columnCapabilities.put(columnName, column.getCapabilities());
      Map<String, String> descs = column.getColumnDescs();
      if (!GuavaUtils.isNullOrEmpty(descs)) {
        columnDescriptors.put(columnName, descs);
      }
    }
    columnTypes.put(Column.TIME_COLUMN_NAME, index.getColumnType(Column.TIME_COLUMN_NAME));
    columnCapabilities.put(Column.TIME_COLUMN_NAME, index.getColumn(Column.TIME_COLUMN_NAME).getCapabilities());
    virtualColumns.addImplicitVCs(this);
  }

  private RowResolver(List<DimensionSpec> dimensions, List<AggregatorFactory> metrics, VirtualColumns virtualColumns)
  {
    this.dimensionNames = Lists.newArrayList();
    this.metricNames = Lists.newArrayList();
    this.virtualColumns = virtualColumns;
    this.aggregators = Maps.newHashMap();
    for (DimensionSpec dimension : dimensions) {
      if (dimension.getExtractionFn() != null) {
        columnTypes.put(dimension.getOutputName(), ValueDesc.ofDimension(ValueType.STRING));
      }
    }
    for (AggregatorFactory metric : metrics) {
      columnTypes.put(metric.getName(), ValueDesc.of(metric.getTypeName()));
    }
    columnTypes.put(Column.TIME_COLUMN_NAME, ValueDesc.LONG);
    for (DimensionSpec dimension : dimensions) {
      if (dimension.getExtractionFn() == null) {
        columnTypes.put(dimension.getOutputName(), dimension.resolveType(this));
      }
    }
    virtualColumns.addImplicitVCs(this);
  }

  private RowResolver(Schema schema, VirtualColumns virtualColumns)
  {
    this.dimensionNames = schema.getDimensionNames();
    this.metricNames = schema.getMetricNames();
    this.virtualColumns = virtualColumns;
    this.aggregators = Maps.newHashMap();
    for (Pair<String, ValueDesc> pair : schema.columnAndTypes()) {
      columnTypes.put(pair.lhs, pair.rhs);
    }
    columnTypes.put(Column.TIME_COLUMN_NAME, ValueDesc.LONG);
    virtualColumns.addImplicitVCs(this);
  }

  @VisibleForTesting
  public RowResolver(Map<String, ValueDesc> columnTypes, VirtualColumns virtualColumns)
  {
    this.dimensionNames = Lists.newArrayList();
    this.metricNames = Lists.newArrayList();
    this.aggregators = Maps.newHashMap();
    this.virtualColumns = virtualColumns;
    this.columnTypes.putAll(columnTypes);
    for (Map.Entry<String, ValueDesc> entry : columnTypes.entrySet()) {
      if (entry.getValue().isDimension()) {
        columnCapabilities.put(
            entry.getKey(),
            new ColumnCapabilitiesImpl().setType(ValueType.STRING).setHasBitmapIndexes(true)
        );
      }
    }
    virtualColumns.addImplicitVCs(this);
  }

  public List<String> getDimensionNames()
  {
    return dimensionNames;
  }

  public List<ValueDesc> getDimensionTypes()
  {
    return resolveColumns(dimensionNames);
  }

  public List<String> getMetricNames()
  {
    return metricNames;
  }

  public List<ValueDesc> getMetricTypes()
  {
    return resolveColumns(metricNames);
  }

  public List<String> getColumnNames()
  {
    return GuavaUtils.concat(dimensionNames, metricNames);
  }

  public boolean isDimension(String columnName)
  {
    return dimensionNames.contains(columnName);
  }

  public boolean isMetric(String columnName)
  {
    return metricNames.contains(columnName);
  }

  public Map<String, AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  public List<AggregatorFactory> getAggregatorsList()
  {
    return Lists.newArrayList(aggregators.values());
  }

  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return columnCapabilities.get(column);
  }

  public Map<String, Map<String, String>> getDescriptors()
  {
    return columnDescriptors;
  }

  public Map<String, String> getDescriptor(String column)
  {
    return columnDescriptors.get(column);
  }

  public VirtualColumn getVirtualColumn(String columnName)
  {
    return virtualColumns.getVirtualColumn(columnName);
  }

  @Override
  public ValueDesc apply(String input)
  {
    return resolveColumn(input);
  }

  @Override
  public ValueDesc resolveColumn(String column)
  {
    return resolveColumn(column, null);
  }

  @Override
  public ValueDesc resolveColumn(String column, ValueDesc defaultType)
  {
    ValueDesc columnType = columnTypes.get(column);
    if (columnType != null) {
      return columnType;
    }
    Pair<VirtualColumn, ValueDesc> vcResolved = virtualColumnTypes.get(column);
    if (vcResolved != null) {
      return vcResolved.rhs;
    }
    VirtualColumn vc = virtualColumns.getVirtualColumn(column);
    if (vc != null) {
      ValueDesc valueType = vc.resolveType(column, this);
      if (valueType != null) {
        virtualColumnTypes.put(column, Pair.of(vc, valueType));
        return valueType;
      }
    }
    return defaultType;
  }

  public VirtualColumn resolveVC(String column)
  {
    if (resolveColumn(column) == null) {
      return null;
    }
    Pair<VirtualColumn, ValueDesc> resolved = virtualColumnTypes.get(column);
    if (resolved != null && resolved.lhs != null) {
      return resolved.lhs;
    }
    return null;
  }

  public Iterable<String> getAllColumnNames()
  {
    Set<String> names = Sets.newLinkedHashSet(virtualColumns.getVirtualColumnNames());
    names.addAll(columnTypes.keySet()); // override
    return names;
  }

  public boolean supportsBitmap(DimFilter filter, EnumSet<BitmapType> using)
  {
    if (filter instanceof RelationExpression) {
      for (Expression child : ((RelationExpression) filter).getChildren()) {
        if (!supportsBitmap((DimFilter) child, using)) {
          return false;
        }
      }
      return true;
    }
    if (filter instanceof MathExprFilter) {
      Expr root = Parser.parse(((MathExprFilter) filter).getExpression());
      return supportsBitmap(Expressions.convertToCNF(root, Parser.EXPR_FACTORY), using);
    }
    Set<String> dependents = Filters.getDependents(filter);
    if (dependents.size() != 1) {
      return false;
    }
    final String column = Iterables.getOnlyElement(dependents);
    if (using.contains(BitmapType.DIMENSIONAL) && supportsBitmap(column, BitmapType.DIMENSIONAL)) {
      return true;
    }
    if (using.contains(BitmapType.LUCENE_INDEX) && supportsBitmap(column, BitmapType.LUCENE_INDEX)) {
      return filter instanceof DimFilter.LuceneFilter;
    }
    if (using.contains(BitmapType.HISTOGRAM_BITMAP) && supportsBitmap(column, BitmapType.HISTOGRAM_BITMAP) ||
        using.contains(BitmapType.BSB) && supportsBitmap(column, BitmapType.BSB)) {
      return filter instanceof DimFilter.RangeFilter && ((DimFilter.RangeFilter)filter).toRanges() != null;
    }
    return false;
  }

  private static final Set<String> BINARY_OPS = Sets.newHashSet("==", "<", ">", "=>", "<=", "in", "between");

  private boolean supportsBitmap(Expr expr, EnumSet<BitmapType> using)
  {
    if (expr instanceof RelationExpression) {
      for (Expression child : ((RelationExpression) expr).getChildren()) {
        if (!supportsBitmap((Expr) child, using)) {
          return false;
        }
      }
      return true;
    }
    if (expr instanceof Expression.FuncExpression) {
      Expression.FuncExpression function = (Expression.FuncExpression) expr;
      List<Expression> children = function.getChildren();
      if (!BINARY_OPS.contains(function.op()) || children.isEmpty()) {
        return false;
      }
      final Expr arg = (Expr) children.get(0);
      if (!Evals.isIdentifier(arg) || !supportsBitmap(arg.toString(), using)) {
        return false;
      }
      for (int i = 1; i < children.size(); i++) {
        if (!Evals.isConstant((Expr) children.get(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private boolean supportsBitmap(String column, BitmapType type, BitmapType... types)
  {
    return supportsBitmap(column, EnumSet.of(type, types));
  }

  private boolean supportsBitmap(String column, EnumSet<BitmapType> using)
  {
    String field = null;
    ColumnCapabilities capabilities = columnCapabilities.get(column);
    if (capabilities == null && column.indexOf('.') > 0) {
      // struct type (mostly for lucene)
      int index = column.indexOf('.');
      field = column.substring(index + 1);
      column = column.substring(0, index);
      capabilities = columnCapabilities.get(column);
    }
    if (capabilities == null) {
      return false;   // dimension type does not asserts existence of bitmap (incremental index, for example)
    }
    if (using.contains(BitmapType.DIMENSIONAL) && capabilities.hasBitmapIndexes()) {
      return true;
    }
    if (using.contains(BitmapType.LUCENE_INDEX) && capabilities.hasLuceneIndex()) {
      final Map<String, String> descriptor = getDescriptor(column);
      return descriptor != null && descriptor.get(field != null ? field : column) != null;
    }
    if (using.contains(BitmapType.HISTOGRAM_BITMAP) && capabilities.hasMetricBitmap()) {
      return true;
    }
    if (using.contains(BitmapType.BSB) && capabilities.hasBitSlicedBitmap()) {
      return true;
    }
    return false;
  }

  public boolean supportsExactBitmap(Iterable<String> columns, DimFilter filter)
  {
    for (String column : columns) {
      if (!supportsBitmap(column, BitmapType.EXACT)) {
        return false;
      }
    }
    return filter == null || supportsBitmap(filter, BitmapType.EXACT);
  }

  public Schema toSubSchema(List<String> columns)
  {
    final List<String> dimensions = Lists.newArrayList();
    final List<ValueDesc> dimensionTypes = Lists.newArrayList();
    final List<String> metrics = Lists.newArrayList();
    final List<ValueDesc> metricTypes = Lists.newArrayList();
    final List<AggregatorFactory> aggregators = Lists.newArrayList();
    for (String column : columns) {
      ValueDesc resolved = resolveColumn(column, ValueDesc.UNKNOWN);
      if (ValueDesc.isDimension(resolved)) {
        dimensions.add(column);
        dimensionTypes.add(resolved);
      } else {
        metrics.add(column);
        metricTypes.add(resolved);
        aggregators.add(getAggregators().get(column));  // can be null
      }
    }
    return new Schema(
        dimensions,
        metrics,
        GuavaUtils.concat(dimensionTypes, metricTypes),
        aggregators,
        columnDescriptors
    );
  }


  public List<ValueDesc> resolveDimensions(List<DimensionSpec> dimensionSpecs)
  {
    List<ValueDesc> types = Lists.newArrayList();
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      types.add(dimensionSpec.resolveType(this));
    }
    return types;
  }

  public List<ValueDesc> resolveColumns(List<String> columns)
  {
    return Lists.newArrayList(Iterables.transform(columns, this));
  }
}
