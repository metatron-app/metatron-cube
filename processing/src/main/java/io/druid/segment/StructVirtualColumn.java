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
import io.druid.java.util.common.IAE;
import io.druid.query.dimension.DimensionSpec;

import java.util.Objects;

/**
 */
public class StructVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x05;

  public static StructVirtualColumn implicit(String metric)
  {
    return new StructVirtualColumn(metric, metric);
  }

  private final String columnName;
  private final String outputName;

  @JsonCreator
  public StructVirtualColumn(
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
    ValueDesc columnType = types.resolve(columnName);
    if (columnType == null || column.equals(outputName)) {
      return columnType;
    }
    Preconditions.checkArgument(column.charAt(outputName.length()) == '.');
    String expression = column.substring(outputName.length() + 1);
    return VirtualColumn.nested(columnType, expression);
  }

  @Override
  public ObjectColumnSelector asMetric(String dimension, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(dimension.startsWith(outputName));
    ObjectColumnSelector selector = factory.makeObjectColumnSelector(columnName);
    if (selector == null || dimension.equals(outputName)) {
      return selector;
    }
    Preconditions.checkArgument(dimension.charAt(outputName.length()) == '.');
    String expression = dimension.substring(outputName.length() + 1);
    if (selector instanceof ComplexColumnSelector.Nested) {
      return ((ComplexColumnSelector.Nested) selector).selector(expression);
    }
    return VirtualColumn.nested(selector, expression);
  }

  @Override
  public FloatColumnSelector asFloatMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    final ValueDesc type = selector.type();
    if (type.isMap() || type.isStruct() || type.isArray()) {
      throw new IAE("%s cannot be used as a float", type);
    }
    return ColumnSelectors.asFloat(selector);
  }

  @Override
  public DoubleColumnSelector asDoubleMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    final ValueDesc type = selector.type();
    if (type.isMap() || type.isStruct() || type.isArray()) {
      throw new IAE("%s cannot be used as a double", type);
    }
    return ColumnSelectors.asDouble(selector);
  }

  @Override
  public LongColumnSelector asLongMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    final ValueDesc type = selector.type();
    if (type.isMap() || type.isStruct() || type.isArray()) {
      throw new IAE("%s cannot be used as a long", type);
    }
    return ColumnSelectors.asLong(selector);
  }

  @Override
  public DimensionSelector asDimension(DimensionSpec dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension.getDimension(), factory);
    final ValueDesc type = selector.type();
    if (type.isMap() || type.isStruct() || type.isArray()) {
      throw new IAE("%s cannot be used as a dimension", type);
    }
    return VirtualColumns.toDimensionSelector(selector, dimension.getExtractionFn());
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new StructVirtualColumn(columnName, outputName);
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
    if (!(o instanceof StructVirtualColumn)) {
      return false;
    }

    StructVirtualColumn that = (StructVirtualColumn) o;

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
    return Objects.hash(columnName, outputName);
  }

  @Override
  public String toString()
  {
    return "StructVirtualColumn{" +
           "columnName='" + columnName + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
