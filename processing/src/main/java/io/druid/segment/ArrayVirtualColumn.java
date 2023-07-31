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
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.dimension.DimensionSpec;

/**
 */
public class ArrayVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x04;

  public static VirtualColumn implicit(String metric)
  {
    return new ArrayVirtualColumn(metric, metric);
  }

  private final String columnName;
  private final String outputName;

  @JsonCreator
  public ArrayVirtualColumn(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("outputName") String outputName
  )
  {
    this.columnName = Preconditions.checkNotNull(columnName, "columnName should not be null");
    this.outputName = outputName == null ? columnName : outputName;
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    Preconditions.checkArgument(column.startsWith(outputName));
    ValueDesc resolved = types.resolve(columnName);
    if (resolved == null || column.equals(outputName)) {
      return resolved;
    }
    String expression = column.substring(outputName.length() + 1);
    return NestedTypes.resolve(resolved, expression);
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(column.startsWith(outputName));
    ObjectColumnSelector selector = factory.makeObjectColumnSelector(columnName);
    if (selector == null || column.equals(outputName)) {
      return selector;
    }
    Preconditions.checkArgument(column.charAt(outputName.length()) == '.');
    String expression = column.substring(outputName.length() + 1);
    if (selector instanceof ComplexColumnSelector.Nested) {
      return ((ComplexColumnSelector.Nested) selector).selector(expression);
    }
    return NestedTypes.resolve(selector, expression);
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
    if (selector == null || !selector.type().isString()) {
      throw new UnsupportedOperationException(dimension + " cannot be used as dimension");
    }
    return VirtualColumns.toDimensionSelector(selector, dimension.getExtractionFn());
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new ArrayVirtualColumn(columnName, outputName);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(VC_TYPE_ID)
                  .append(columnName, outputName);
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
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

    if (!columnName.equals(that.columnName)) {
      return false;
    }
    if (!outputName.equals(that.outputName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = columnName.hashCode();
    result = 31 * result + outputName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "ArrayVirtualColumn{" +
           "columnName='" + columnName + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
