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
import com.google.common.collect.ImmutableSet;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.UOE;
import io.druid.query.dimension.DimensionSpec;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 */
public class ArrayVirtualColumn implements VirtualColumn.IndexProvider.Rewritable
{
  private final KeyIndexedVirtualColumn keyIndexed;

  public static VirtualColumn implicit(String metric, ValueDesc type)
  {
    return new ArrayVirtualColumn(metric, type);
  }

  private final String columnName;
  private final ValueDesc type;

  @JsonCreator
  public ArrayVirtualColumn(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("type") ValueDesc type
  )
  {
    this.columnName = Preconditions.checkNotNull(columnName, "columnName should not be null");
    this.keyIndexed = null;
    this.type = type;
  }

  private ArrayVirtualColumn(String columnName, ValueDesc type, KeyIndexedVirtualColumn indexer)
  {
    this.columnName = columnName;
    this.keyIndexed = indexer;
    this.type = type;
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    Preconditions.checkArgument(column.startsWith(columnName));
    if (columnName.equals(column)) {
      return type == null ? ValueDesc.ARRAY : type;
    }
    ValueDesc resolved = types.resolve(columnName);
    if (resolved == null || column.equals(columnName)) {
      return resolved;
    }
    String expression = column.substring(columnName.length() + 1);
    return NestedTypes.resolve(resolved, expression);
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(column.startsWith(columnName));
    if (columnName.equals(column)) {
      return factory.makeObjectColumnSelector(column);
    }
    Preconditions.checkArgument(column.charAt(columnName.length()) == '.');
    for (int ix = columnName.length(); ix > 0; ix = column.indexOf('.', ix + 1)) {
      ObjectColumnSelector selector = factory.makeObjectColumnSelector(column.substring(0, ix));
      if (selector == null) {
        continue;
      }
      Preconditions.checkArgument(column.charAt(columnName.length()) == '.');
      String expression = column.substring(ix + 1);
      if (selector instanceof ComplexColumnSelector.Nested) {
        return ((ComplexColumnSelector.Nested) selector).nested(expression);
      }
      return NestedTypes.resolve(selector, expression);
    }
    return factory.makeObjectColumnSelector(column);
  }

  @Override
  public FloatColumnSelector asFloatMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (ValueDesc.isMap(selector.type())) {
      throw new UnsupportedOperationException("asFloatMetric");
    }
    return ColumnSelectors.asFloat(selector);
  }

  @Override
  public DoubleColumnSelector asDoubleMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (ValueDesc.isMap(selector.type())) {
      throw new UnsupportedOperationException("asDoubleMetric");
    }
    return ColumnSelectors.asDouble(selector);
  }

  @Override
  public LongColumnSelector asLongMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (ValueDesc.isMap(selector.type())) {
      throw new UnsupportedOperationException("asLongMetric");
    }
    return ColumnSelectors.asLong(selector);
  }

  @Override
  public DimensionSelector asDimension(DimensionSpec dimension, ColumnSelectorFactory factory)
  {
    ObjectColumnSelector selector = asMetric(dimension.getDimension(), factory);
    if (selector == null) {
      throw new UOE("%s cannot be used as dimension", dimension.getDimension());
    }
    DimensionSelector dimensions = VirtualColumns.toDimensionSelector(selector, dimension.getExtractionFn());
    if (keyIndexed != null && keyIndexed.getKeyDimension().equals(dimension.getDimension())) {
      dimensions = keyIndexed.wrap(dimensions);
    }
    return dimensions;
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new ArrayVirtualColumn(columnName, type);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder;
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @Override
  public String getOutputName()
  {
    return columnName;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ArrayVirtualColumn)) {
      return false;
    }
    ArrayVirtualColumn that = (ArrayVirtualColumn) o;
    return columnName.equals(that.columnName) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode()
  {
    int result = columnName.hashCode();
    result = 31 * result + columnName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "ArrayVirtualColumn{" +
           "columnName='" + columnName + '\'' +
           ", columnName='" + columnName + '\'' +
           '}';
  }

  @Override
  public String sourceColumn()
  {
    return keyIndexed == null ? null : keyIndexed.sourceColumn();
  }

  @Override
  public Set<String> targetColumns()
  {
    return keyIndexed == null ? ImmutableSet.of() : keyIndexed.targetColumns();
  }

  @Override
  public ColumnSelectorFactory override(ColumnSelectorFactory factory)
  {
    return keyIndexed == null ? factory : keyIndexed.override(factory);
  }

  @Override
  public IndexProvider withIndexer(String keyDimension, List<String> valueMetrics)
  {
    return new ArrayVirtualColumn(
        columnName,
        type,
        KeyIndexedVirtualColumn.implicit(keyDimension, valueMetrics, columnName)
    );
  }
}
