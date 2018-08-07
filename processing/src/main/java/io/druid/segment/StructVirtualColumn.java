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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.StructMetricSerde;

import java.nio.ByteBuffer;
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
    final ValueDesc columnType = types.resolve(columnName);
    Preconditions.checkArgument(columnType.isStruct(), columnName + " is not struct type");
    if (column.equals(outputName)) {
      return columnType;
    }
    final String fieldName = column.substring(columnName.length() + 1);
    final StructMetricSerde serde = (StructMetricSerde) ComplexMetrics.getSerdeForType(columnType);
    return ValueDesc.of(serde.getTypeOf(fieldName));
  }

  @Override
  public ObjectColumnSelector asMetric(String dimension, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(dimension.startsWith(outputName));
    ValueDesc columnType = factory.getColumnType(columnName);
    Preconditions.checkArgument(columnType.isStruct(), columnName + " is not struct type");

    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(columnName);
    if (dimension.equals(outputName)) {
      return selector;
    }
    final String fieldName = dimension.substring(columnName.length() + 1);
    final StructMetricSerde serde = (StructMetricSerde) ComplexMetrics.getSerdeForType(columnType);
    final int index = serde.indexOf(fieldName);
    if (index < 0) {
      return ColumnSelectors.nullObjectSelector(ValueDesc.UNKNOWN);
    }
    ValueType elementType = serde.type(index);
    Preconditions.checkArgument(elementType.isPrimitive(), "only primitives are allowed in struct");

    final ValueDesc fieldType = ValueDesc.of(elementType);
    return new ObjectColumnSelector()
    {
      @Override
      public Object get()
      {
        return ((Object[])selector.get())[index];
      }

      @Override
      public ValueDesc type()
      {
        return fieldType;
      }
    };
  }

  @Override
  public FloatColumnSelector asFloatMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (ValueDesc.isMap(selector.type()) || ValueDesc.isArray(selector.type())) {
      throw new UnsupportedOperationException(selector.type() + " cannot be used as a float");
    }
    return ColumnSelectors.asFloat(selector);
  }

  @Override
  public DoubleColumnSelector asDoubleMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (ValueDesc.isMap(selector.type()) || ValueDesc.isArray(selector.type())) {
      throw new UnsupportedOperationException(selector.type() + " cannot be used as a double");
    }
    return ColumnSelectors.asDouble(selector);
  }

  @Override
  public LongColumnSelector asLongMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (ValueDesc.isMap(selector.type()) || ValueDesc.isArray(selector.type())) {
      throw new UnsupportedOperationException(selector.type() + " cannot be used as a long");
    }
    return ColumnSelectors.asLong(selector);
  }

  @Override
  public DimensionSelector asDimension(String dimension, ExtractionFn extractionFn, ColumnSelectorFactory factory)
  {
    ObjectColumnSelector selector = asMetric(dimension, factory);
    if (ValueDesc.isMap(selector.type()) || ValueDesc.isArray(selector.type())) {
      throw new UnsupportedOperationException(selector.type() + " cannot be used as a Dimension");
    }
    return VirtualColumns.toDimensionSelector(selector, extractionFn);
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new StructVirtualColumn(columnName, outputName);
  }


  @Override
  public byte[] getCacheKey()
  {
    byte[] columnNameBytes = StringUtils.toUtf8(columnName);
    byte[] outputNameBytes = StringUtils.toUtf8(outputName);

    return ByteBuffer.allocate(2 + columnNameBytes.length + outputNameBytes.length)
                     .put(VC_TYPE_ID)
                     .put(columnNameBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(outputNameBytes)
                     .array();
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public boolean isIndexed(String dimension)
  {
    return false;
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
