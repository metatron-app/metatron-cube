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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.Query;
import io.druid.query.RowResolver;
import io.druid.query.RowSignature;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.VirtualColumns;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
public class Schema implements TypeResolver, RowSignature
{
  public static final Schema EMPTY = new Schema(
      Collections.<String>emptyList(),
      Collections.<String>emptyList(),
      Collections.<ValueDesc>emptyList(),
      Collections.<AggregatorFactory>emptyList()
  );

  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final List<ValueDesc> columnTypes;
  private final List<AggregatorFactory> aggregators;

  @JsonCreator
  public Schema(
      @JsonProperty("dimensionNames") List<String> dimensionNames,
      @JsonProperty("metricNames") List<String> metricNames,
      @JsonProperty("columnTypes") List<ValueDesc> columnTypes,
      @JsonProperty("aggregators") List<AggregatorFactory> aggregators
  )
  {
    this.dimensionNames = Preconditions.checkNotNull(dimensionNames);
    this.metricNames = Preconditions.checkNotNull(metricNames);
    this.columnTypes = Preconditions.checkNotNull(columnTypes);
    this.aggregators = Preconditions.checkNotNull(aggregators);
    Preconditions.checkArgument(dimensionNames.size() == Sets.newHashSet(dimensionNames).size());
    Preconditions.checkArgument(metricNames.size() == Sets.newHashSet(metricNames).size());
    Preconditions.checkArgument(dimensionNames.size() + metricNames.size() == columnTypes.size());
    Preconditions.checkArgument(metricNames.size() == aggregators.size());
  }

  @JsonProperty
  public List<String> getDimensionNames()
  {
    return dimensionNames;
  }

  @JsonProperty
  public List<String> getMetricNames()
  {
    return metricNames;
  }

  public List<String> getColumnNames()
  {
    return Lists.newArrayList(Iterables.concat(dimensionNames, metricNames));
  }

  @JsonProperty
  public List<ValueDesc> getColumnTypes()
  {
    return columnTypes;
  }

  @JsonProperty
  public List<AggregatorFactory> getAggregators()
  {
    return aggregators;
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

  public Iterable<Pair<String, AggregatorFactory>> metricAndAggregators()
  {
    return GuavaUtils.zip(metricNames, aggregators);
  }

  public AggregatorFactory getAggregatorOfName(String metric)
  {
    int index = metricNames.indexOf(metric);
    return index < 0 ? null : aggregators.get(index);
  }

  @Override
  public ValueDesc resolveColumn(String column)
  {
    int index = dimensionNames.indexOf(column);
    if (index < 0) {
      index = metricNames.indexOf(column) + dimensionNames.size();
    }
    return columnTypes.get(index);
  }

  @Override
  public ValueDesc resolveColumn(String column, ValueDesc defaultType)
  {
    return Optional.fromNullable(resolveColumn(column)).or(defaultType);
  }

  public Schema appendTime()
  {
    return new Schema(
        Lists.newArrayList(dimensionNames),
        GuavaUtils.concat(metricNames, EventHolder.timestampKey),
        GuavaUtils.concat(columnTypes, ValueDesc.LONG),
        GuavaUtils.concat(aggregators, (AggregatorFactory) null)
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
    List<AggregatorFactory> mergedAggregators = Lists.newArrayList();
    for (String metric : mergedMetrics) {
      AggregatorFactory factory1 = getAggregatorOfName(metric);
      AggregatorFactory factory2 = other.getAggregatorOfName(metric);
      if (factory1 == null) {
        mergedAggregators.add(factory2);
      } else if (factory2 == null) {
        mergedAggregators.add(factory1);
      } else {
        try {
          AggregatorFactory factory = factory1.getMergingFactory(factory2);
          merged.put(metric, ValueDesc.of(factory.getTypeName()));
          mergedAggregators.add(factory);
        }
        catch (AggregatorFactoryNotMergeableException e) {
          // fucked
          mergedAggregators.add(null);
        }
      }
    }
    List<ValueDesc> mergedTypes = Lists.newArrayList();
    for (String columnName : Iterables.concat(mergedDimensions, mergedMetrics)) {
      ValueDesc type1 = resolveColumn(columnName);
      ValueDesc type2 = other.resolveColumn(columnName);
      if (!type1.equals(type2)) {
        ValueDesc type = merged.get(columnName);
        mergedTypes.add(type == null ? ValueDesc.UNKNOWN : type);
      } else {
        mergedTypes.add(type1);
      }
    }

    return new Schema(mergedDimensions, mergedMetrics, mergedTypes, mergedAggregators);
  }

  public List<ValueDesc> resolveDimensionTypes(Query.DimensionSupport<?> query)
  {
    List<ValueDesc> types = Lists.newArrayList();
    RowResolver resolver = RowResolver.of(this, VirtualColumns.valueOf(query.getVirtualColumns()));
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      types.add(dimensionSpec.resolveType(resolver));
    }
    return types;
  }

  public List<AggregatorFactory> resolveMetricTypes(Query.MetricSupport<?> query)
  {
    List<AggregatorFactory> types = Lists.newArrayList();
    for (String metric : query.getMetrics()) {
      int index = metricNames.indexOf(metric);
      if (index >= 0) {
        types.add(aggregators.get(index));
      }
    }
    return types;
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

    if (!aggregators.equals(schema.aggregators)) {
      return false;
    }
    if (!columnTypes.equals(schema.columnTypes)) {
      return false;
    }
    if (!dimensionNames.equals(schema.dimensionNames)) {
      return false;
    }
    if (!metricNames.equals(schema.metricNames)) {
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
    result = 31 * result + aggregators.hashCode();
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
           '}';
  }
}
