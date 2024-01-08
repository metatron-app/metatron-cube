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
import com.google.common.collect.Lists;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.ComplexColumnSelector.FromMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static io.druid.data.input.Row.MAP_KEY;
import static io.druid.data.input.Row.MAP_VALUE;

/**
 *
 */
public class ImplicitMapVirtualColumn implements VirtualColumn.IndexProvider
{
  private final String metric;
  private final ValueDesc type;
  private final KeyIndexedVirtualColumn keyIndexed;

  public ImplicitMapVirtualColumn(String metric, ValueDesc type)
  {
    this.metric = metric;
    this.type = type;
    this.keyIndexed = new KeyIndexedVirtualColumn(
        metric + "." + MAP_KEY,
        null,
        Arrays.asList(metric + "." + MAP_VALUE),
        null,
        metric
    );
    Preconditions.checkArgument(type == null || type.isMap());
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver resolver)
  {
    Preconditions.checkArgument(column.startsWith(metric));
    if (metric.equals(column)) {
      return type == null ? ValueDesc.MAP : type;
    }
    ValueDesc type = resolver.resolve(metric);
    if (type == null || metric.equals(column)) {
      return type;
    }
    String expression = column.substring(metric.length() + 1);
    return NestedTypes.resolve(type, expression);
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(column.startsWith(metric));
    ObjectColumnSelector selector = factory.makeObjectColumnSelector(metric);
    if (selector == null || metric.equals(column)) {
      return selector;
    }
    Preconditions.checkArgument(column.charAt(metric.length()) == '.');
    String expression = column.substring(metric.length() + 1);
    if (selector instanceof ComplexColumnSelector.Nested) {
      return ((ComplexColumnSelector.Nested) selector).nested(expression);
    }
    return NestedTypes.resolve(selector, expression);
  }

  @Override
  public DimensionSelector asDimension(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
  {
    String dimension = dimensionSpec.getDimension();
    ExtractionFn extractionFn = dimensionSpec.getExtractionFn();
    Preconditions.checkArgument(dimension.startsWith(metric));

    ObjectColumnSelector selector = factory.makeObjectColumnSelector(metric);
    if (selector == null || metric.equals(dimension)) {
      return VirtualColumns.toDimensionSelector(selector, extractionFn);
    }
    Preconditions.checkArgument(dimension.charAt(metric.length()) == '.');
    ValueDesc type = selector.type();
    if (!type.isMap()) {
      return null;
    }
    String postfix = dimension.substring(metric.length() + 1);
    if (MAP_KEY.equals(postfix)) {
      if (selector instanceof ComplexColumnSelector.FromMap) {
        ScanContext context = factory instanceof Cursor ? ((Cursor) factory).scanContext() : null;  // todo
        return ((FromMap) selector).keyDimensionSelector(context, extractionFn, keyIndexed.indexer());
      }
      ObjectColumnSelector values = ObjectColumnSelector.typed(ValueDesc.MV_STRING, () ->
      {
        Object value = selector.get();
        if (value instanceof Map) {
          return Lists.newArrayList(((Map) value).keySet());
        }
        return null;
      });
      return keyIndexed.wrap(VirtualColumns.toDimensionSelector(values, extractionFn));
    }
    if (MAP_VALUE.equals(postfix)) {
      if (selector instanceof ComplexColumnSelector.FromMap) {
        ObjectColumnSelector values = ((FromMap) selector).valueSelector();
        return VirtualColumns.toDimensionSelector(values, extractionFn);
      }
      String[] description = TypeUtils.splitDescriptiveType(type);
      ValueDesc valueType = description == null ? ValueDesc.ARRAY : ValueDesc.ofArray(description[2]);
      ObjectColumnSelector values = ObjectColumnSelector.typed(valueType, () -> {
        Object value = selector.get();
        if (value instanceof Map) {
          return Lists.newArrayList(((Map) value).values());
        }
        return null;
      });
      return VirtualColumns.toDimensionSelector(values, extractionFn);
    }
    return NullDimensionSelector.of(ValueDesc.UNKNOWN, extractionFn);
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new ImplicitMapVirtualColumn(metric, type);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder;
  }

  @Override
  public String sourceColumn()
  {
    return keyIndexed.sourceColumn();
  }

  @Override
  public Set<String> targetColumns()
  {
    return keyIndexed.targetColumns();
  }

  @Override
  public ColumnSelectorFactory override(ColumnSelectorFactory factory)
  {
    return keyIndexed.override(factory);
  }

  @Override
  public String getOutputName()
  {
    return metric;
  }

  @Override
  public boolean equals(Object o)
  {
    return o instanceof ImplicitMapVirtualColumn && metric.equals(((ImplicitMapVirtualColumn) o).metric);
  }

  @Override
  public int hashCode()
  {
    return metric.hashCode();
  }

  @Override
  public String toString()
  {
    return "ImplicitMapVirtualColumn{metric='" + metric + '\'' + '}';
  }
}
