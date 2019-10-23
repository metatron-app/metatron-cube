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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.Pair;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.Granularities;
import io.druid.math.expr.ExprType;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.RowResolver;
import io.druid.query.RowSignature;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class Schema implements TypeResolver, RowSignature
{
  public static final Schema EMPTY = new Schema(
      Collections.<String>emptyList(),
      Collections.<String>emptyList(),
      Collections.<ValueDesc>emptyList(),
      Collections.<String, AggregatorFactory>emptyMap(),
      Collections.<String, ColumnCapabilities>emptyMap(),
      Collections.<String, Map<String, String>>emptyMap()
  );

  @VisibleForTesting
  public static Schema of(Map<String, ValueDesc> columnAndTypes)
  {
    List<String> dimensions = Lists.newArrayList();
    List<String> metrics = Lists.newArrayList();
    List<ValueDesc> columnTypes = Lists.newArrayList();
    Map<String, ColumnCapabilities> capabilities = Maps.newHashMap();
    for (Map.Entry<String, ValueDesc> entry : columnAndTypes.entrySet()) {
      String column = entry.getKey();
      ValueDesc type = entry.getValue();
      if (column.equals(Column.TIME_COLUMN_NAME)) {
        dimensions.add(column);
        capabilities.put(column, ColumnCapabilities.of(ValueType.LONG));
        columnTypes.add(ValueDesc.LONG);
        continue;
      }
      if (type.isDimension()) {
        dimensions.add(column);
        capabilities.put(column, ColumnCapabilities.of(ValueType.STRING).setHasBitmapIndexes(true));
      } else {
        metrics.add(column);
      }
      columnTypes.add(type);
    }
    return new Schema(dimensions, metrics, columnTypes, null, capabilities, null);
  }

  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final List<ValueDesc> columnTypes;
  private final Map<String, AggregatorFactory> aggregators;
  private final Map<String, ColumnCapabilities> capabilities;
  private final Map<String, Map<String, String>> descriptors;

  @JsonCreator
  public Schema(
      @JsonProperty("dimensionNames") List<String> dimensionNames,
      @JsonProperty("metricNames") List<String> metricNames,
      @JsonProperty("columnTypes") List<ValueDesc> columnTypes,
      @JsonProperty("aggregators") Map<String, AggregatorFactory> aggregators,
      @JsonProperty("capabilities") Map<String, ColumnCapabilities> capabilities,
      @JsonProperty("descriptors") Map<String, Map<String, String>> descriptors
  )
  {
    this.dimensionNames = Preconditions.checkNotNull(dimensionNames);
    this.metricNames = Preconditions.checkNotNull(metricNames);
    this.columnTypes = Preconditions.checkNotNull(columnTypes);
    this.aggregators = aggregators == null ? ImmutableMap.<String, AggregatorFactory>of() : aggregators;
    this.capabilities = capabilities == null ? ImmutableMap.<String, ColumnCapabilities>of() : capabilities;
    this.descriptors = descriptors == null ? ImmutableMap.<String, Map<String, String>>of() : descriptors;
    Preconditions.checkArgument(dimensionNames.size() + metricNames.size() == columnTypes.size());
  }

  public Schema(List<String> dimensions, List<String> metrics, List<ValueDesc> types)
  {
    this(dimensions, metrics, types, null, null, null);
  }

  @JsonProperty
  public List<String> getDimensionNames()
  {
    return dimensionNames;
  }

  public List<ValueDesc> getDimensionTypes()
  {
    return columnTypes.subList(0, dimensionNames.size());
  }

  @JsonProperty
  public List<String> getMetricNames()
  {
    return metricNames;
  }

  public List<ValueDesc> getMetricTypes()
  {
    return columnTypes.subList(dimensionNames.size(), columnTypes.size());
  }

  public List<ValueDesc> getColumnTypes(List<String> columns)
  {
    List<ValueDesc> types = Lists.newArrayList();
    List<String> columnNames = getColumnNames();
    for (String column : columns) {
      final int index = columnNames.indexOf(column);
      types.add(index >= 0 ? columnTypes.get(index) : ValueDesc.UNKNOWN);
    }
    return types;
  }

  @Override
  public List<String> getColumnNames()
  {
    return Lists.newArrayList(Iterables.concat(dimensionNames, metricNames));
  }

  @Override
  @JsonProperty
  public List<ValueDesc> getColumnTypes()
  {
    return columnTypes;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, ColumnCapabilities> getCapabilities()
  {
    return capabilities;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Map<String, String>> getDescriptors()
  {
    return descriptors;
  }

  public int size()
  {
    return columnTypes.size();
  }

  public Iterable<Pair<String, ValueDesc>> columnAndTypes()
  {
    return GuavaUtils.zip(getColumnNames(), columnTypes);
  }

  public Iterable<Pair<String, ValueDesc>> dimensionAndTypes()
  {
    return GuavaUtils.zip(dimensionNames, columnTypes.subList(0, dimensionNames.size()));
  }

  public Iterable<Pair<String, ValueDesc>> metricAndTypes()
  {
    return GuavaUtils.zip(metricNames, columnTypes.subList(dimensionNames.size(), columnTypes.size()));
  }

  public String dimensionAndTypesString()
  {
    return toString(dimensionAndTypes());
  }

  public String metricAndTypesString()
  {
    return toString(metricAndTypes());
  }

  public String columnAndTypesString()
  {
    return toString(columnAndTypes());
  }

  public String asTypeString()
  {
    final StringBuilder s = new StringBuilder();
    for (Pair<String, ValueDesc> pair : columnAndTypes()) {
      if (s.length() > 0) {
        s.append(',');
      }
      s.append(pair.lhs).append(':').append(ExprType.toTypeString(pair.rhs));
    }
    return s.toString();
  }

  private String toString(Iterable<Pair<String, ValueDesc>> nameAndTypes)
  {
    StringBuilder builder = new StringBuilder();
    for (Pair<String, ValueDesc> pair : nameAndTypes) {
      if (builder.length() > 0) {
        builder.append(',');
      }
      builder.append(pair.lhs).append(':').append(pair.rhs);
    }
    return builder.toString();
  }

  @Override
  public ValueDesc resolve(String column)
  {
    int index = dimensionNames.indexOf(column);
    if (index < 0) {
      index = metricNames.indexOf(column);
      if (index >= 0) {
        index += dimensionNames.size();
      }
    }
    return index < 0 ? null : columnTypes.get(index);
  }

  @Override
  public ValueDesc resolve(String column, ValueDesc defaultType)
  {
    final ValueDesc resolved = resolve(column);
    return resolved == null ? defaultType : resolved;
  }

  public ColumnCapabilities getColumnCapability(String column)
  {
    return capabilities.get(column);
  }

  public Map<String, String> getColumnDescriptor(String column)
  {
    return descriptors.get(column);
  }

  public Schema withDimensions(List<String> dimensionNames, List<ValueDesc> dimensionTypes)
  {
    return new Schema(
        dimensionNames,
        metricNames,
        GuavaUtils.<ValueDesc>concat(dimensionTypes, getMetricTypes())
    );
  }

  public Schema withMetrics(List<String> metricNames, List<ValueDesc> metricTypes)
  {
    return new Schema(
        dimensionNames,
        metricNames,
        GuavaUtils.concat(getDimensionTypes(), metricTypes)
    );
  }

  public Schema appendMetrics(String metricName, ValueDesc metricType)
  {
    return new Schema(
        dimensionNames,
        GuavaUtils.concat(getMetricNames(), metricName),
        GuavaUtils.concat(getColumnTypes(), metricType)
    );
  }

  public Schema appendMetrics(List<String> metricNames, List<ValueDesc> metricTypes)
  {
    return new Schema(
        dimensionNames,
        GuavaUtils.concat(getMetricNames(), metricNames),
        GuavaUtils.concat(getColumnTypes(), metricTypes)
    );
  }

  public Schema merge(Schema other)
  {
    List<String> mergedDimensions = Lists.newArrayList(dimensionNames);
    for (String dimension : other.getDimensionNames()) {
      if (!mergedDimensions.contains(dimension)) {
        mergedDimensions.add(dimension);
      }
    }
    List<String> mergedMetrics = Lists.newArrayList(metricNames);
    for (String metric : other.getMetricNames()) {
      if (!mergedMetrics.contains(metric)) {
        mergedMetrics.add(metric);
      }
    }
    Map<String, ValueDesc> merged = Maps.newHashMap();
    Map<String, AggregatorFactory> mergedAggregators = Maps.newHashMap();
    for (String metric : mergedMetrics) {
      AggregatorFactory factory1 = aggregators.get(metric);
      AggregatorFactory factory2 = other.aggregators.get(metric);
      if (factory1 == null && factory2 == null) {
        continue;
      }
      if (factory1 == null) {
        mergedAggregators.put(metric, factory2);
      } else if (factory2 == null) {
        mergedAggregators.put(metric, factory1);
      } else {
        try {
          AggregatorFactory factory = factory1.getMergingFactory(factory2);
          merged.put(metric, factory.getOutputType());
          mergedAggregators.put(metric, factory);
        }
        catch (AggregatorFactoryNotMergeableException e) {
          // fucked
        }
      }
    }
    List<ValueDesc> mergedTypes = Lists.newArrayList();
    for (String columnName : Iterables.concat(mergedDimensions, mergedMetrics)) {
      ValueDesc type1 = resolve(columnName);
      ValueDesc type2 = other.resolve(columnName);
      if (Objects.equals(type1, type2)) {
        mergedTypes.add(type1);
      } else if (type1 != null && type2 == null) {
        mergedTypes.add(merged.getOrDefault(columnName, type1));
      } else if (type1 == null && type2 != null) {
        mergedTypes.add(merged.getOrDefault(columnName, type2));
      } else {
        mergedTypes.add(merged.getOrDefault(columnName, ValueDesc.toCommonType(type1, type2)));
      }
    }
    Map<String, ColumnCapabilities> capabilitiesMap = Maps.newHashMap();
    for (String columnName : GuavaUtils.concat(mergedDimensions, mergedMetrics)) {
      ColumnCapabilities cap1 = capabilities.get(columnName);
      ColumnCapabilities cap2 = other.capabilities.get(columnName);
      if (cap1 == null && cap2 == null) {
        continue;
      }
      if (cap1 == null) {
        capabilitiesMap.put(columnName, cap2);
      } else if (cap2 == null || cap1.equals(cap2)) {
        capabilitiesMap.put(columnName, cap1);
      }
    }
    Map<String, Map<String, String>> mergedDescs = Maps.newLinkedHashMap();
    for (String columnName : GuavaUtils.concat(mergedDimensions, mergedMetrics)) {
      Map<String, String> desc1 = descriptors.get(columnName);
      Map<String, String> desc2 = other.descriptors.get(columnName);
      if (desc1 == null && desc2 == null) {
        continue;
      }
      if (desc1 == null) {
        mergedDescs.put(columnName, desc2);
      } else if (desc2 == null || desc1.equals(desc2)) {
        mergedDescs.put(columnName, desc1);
      }
    }

    return new Schema(mergedDimensions, mergedMetrics, mergedTypes, mergedAggregators, capabilitiesMap, mergedDescs);
  }

  public Schema resolve(Query<?> query, boolean finalzed)
  {
    List<String> dimensions = Lists.newArrayList();
    List<String> metrics = Lists.newArrayList();
    List<ValueDesc> dimensionTypes = Lists.newArrayList();
    List<ValueDesc> metricTypes = Lists.newArrayList();

    Schema schema = columnTypes.isEmpty() ?
                    new Schema(dimensions, metrics, GuavaUtils.concatish(dimensionTypes, metricTypes)) : this;

    List<VirtualColumn> virtualColumns = BaseQuery.getVirtualColumns(query);
    TypeResolver resolver = GuavaUtils.isNullOrEmpty(virtualColumns) ? schema : RowResolver.of(schema, virtualColumns);

    if (query instanceof Query.ColumnsSupport) {
      final List<String> columns = ((Query.ColumnsSupport<?>) query).getColumns();
      for (String column : columns) {
        ValueDesc resolved = resolver.resolve(column);
        if (resolved == null || resolved.isDimension()) {
          dimensions.add(column);
          dimensionTypes.add(resolved);
        } else {
          metrics.add(column);
          metricTypes.add(resolved);
        }
      }
      return new Schema(dimensions, metrics, GuavaUtils.concat(dimensionTypes, metricTypes));
    }
    for (DimensionSpec dimensionSpec : BaseQuery.getDimensions(query)) {
      dimensionTypes.add(dimensionSpec.resolve(resolver));
      dimensions.add(dimensionSpec.getOutputName());
    }
    for (String metric : BaseQuery.getMetrics(query)) {
      metricTypes.add(resolver.resolve(metric));
      metrics.add(metric);
    }
    List<AggregatorFactory> aggregators = BaseQuery.getAggregators(query);
    List<PostAggregator> postAggregators = BaseQuery.getPostAggregators(query);
    for (AggregatorFactory metric : aggregators) {
      metricTypes.add(finalzed ? metric.finalizedType() : metric.getOutputType());
      metric = metric.resolveIfNeeded(Suppliers.ofInstance(resolver));
      metrics.add(metric.getName());
    }
    for (PostAggregator postAggregator : PostAggregators.decorate(postAggregators, aggregators)) {
      metricTypes.add(postAggregator.resolve(resolver));
      metrics.add(postAggregator.getName());
    }
    return new Schema(dimensions, metrics, GuavaUtils.concat(dimensionTypes, metricTypes));
  }

  // for streaming sub query.. we don't have index
  public Schema replaceDimensionToString()
  {
    List<ValueDesc> replaced = Lists.newArrayList(columnTypes);
    for (int i = 0; i < replaced.size(); i++) {
      if (ValueDesc.isDimension(replaced.get(i))) {
        replaced.set(i, ValueDesc.STRING);
      }
    }
    return new Schema(dimensionNames, metricNames, replaced);
  }

  // all of nothing
  public List<ValueDesc> tryColumnTypes(List<String> columns)
  {
    List<ValueDesc> columnTypes = Lists.newArrayList();
    for (String column : columns) {
      ValueDesc resolved = resolve(column);
      if (resolved == null || resolved.isUnknown()) {
        return null;
      }
      columnTypes.add(resolved);
    }
    return columnTypes;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Schema schema = (Schema) o;

    if (!dimensionNames.equals(schema.dimensionNames)) {
      return false;
    }
    if (!metricNames.equals(schema.metricNames)) {
      return false;
    }
    if (!columnTypes.equals(schema.columnTypes)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimensionNames.hashCode();
    result = 31 * result + metricNames.hashCode();
    result = 31 * result + columnTypes.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "Schema{" +
           "dimensionNames=" + dimensionNames +
           ", metricNames=" + metricNames +
           ", columnTypes=" + columnTypes +
           ", aggregators=" + aggregators +
           ", descriptors=" + descriptors +
           ", capabilities=" + capabilities +
           '}';
  }

  public IncrementalIndexSchema asRelaySchema()
  {
    // use granularity truncated min timestamp since incoming truncated timestamps may precede timeStart
    return new IncrementalIndexSchema.Builder()
        .withMinTimestamp(Long.MIN_VALUE)
        .withQueryGranularity(Granularities.ALL)
        .withDimensions(getDimensionNames(), getDimensionTypes())
        .withMetrics(AggregatorFactory.toRelay(getMetricNames(), getMetricTypes()))
        .withFixedSchema(true)
        .withRollup(false)
        .build();
  }

  // input to output mapping
  public IncrementalIndexSchema asRelaySchema(Map<String, String> mapping)
  {
    if (GuavaUtils.isNullOrEmpty(mapping)) {
      return asRelaySchema();
    }
    List<DimensionSchema> dimensionSchemas = Lists.newArrayList();
    for (Pair<String, ValueDesc> pair : dimensionAndTypes()) {
      ValueType type = pair.rhs.isStringOrDimension() ? ValueType.STRING : pair.rhs.type();
      dimensionSchemas.add(DimensionSchema.of(mapping.get(pair.lhs), pair.lhs, type));
    }
    List<AggregatorFactory> merics = Lists.newArrayList();
    for (Pair<String, ValueDesc> pair : metricAndTypes()) {
      merics.add(new RelayAggregatorFactory(mapping.get(pair.lhs), pair.lhs, pair.rhs.typeName()));
    }
    // use granularity truncated min timestamp since incoming truncated timestamps may precede timeStart
    return new IncrementalIndexSchema.Builder()
        .withMinTimestamp(Long.MIN_VALUE)
        .withQueryGranularity(Granularities.ALL)
        .withDimensionsSpec(new DimensionsSpec(dimensionSchemas, null, null))
        .withMetrics(merics)
        .withFixedSchema(true)
        .withRollup(false)
        .build();
  }
}
