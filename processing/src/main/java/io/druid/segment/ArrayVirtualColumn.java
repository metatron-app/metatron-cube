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
import com.google.common.primitives.Ints;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.filter.DimFilterCacheHelper;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class ArrayVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x04;

  public static VirtualColumn implicit(String metric)
  {
    return new ArrayVirtualColumn(metric, metric);
  }

  private final String arrayMetric;
  private final String outputName;

  @JsonCreator
  public ArrayVirtualColumn(
      @JsonProperty("arrayMetric") String arrayMetric,
      @JsonProperty("outputName") String outputName
  )
  {
    this.arrayMetric = Preconditions.checkNotNull(arrayMetric, "arrayMetric should not be null");
    this.outputName = Preconditions.checkNotNull(outputName, "outputName should not be null");
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    Preconditions.checkArgument(column.startsWith(outputName));
    int index = column.indexOf('.', outputName.length());
    if (index < 0) {
      return types.resolveColumn(arrayMetric);
    }
    final Integer access = Ints.tryParse(column.substring(index + 1));
    if (access == null || access < 0) {
      throw new IllegalArgumentException("expects index attached in " + column);
    }
    ValueDesc valueDesc = types.resolveColumn(arrayMetric);
    if (ValueDesc.isArray(valueDesc)) {
      return ValueDesc.subElementOf(valueDesc, ValueDesc.UNKNOWN);
    }
    return null;
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(column.startsWith(outputName));
    final int index = column.indexOf('.', outputName.length());
    if (index < 0) {
      return factory.makeObjectColumnSelector(arrayMetric);
    }
    final Integer access = Ints.tryParse(column.substring(index + 1));
    if (access == null || access < 0) {
      throw new IllegalArgumentException("expects index attached in " + column);
    }
    ValueDesc indexed = factory.getColumnType(arrayMetric);
    if (ValueDesc.isArray(indexed)) {
      @SuppressWarnings("unchecked")
      final ObjectColumnSelector<List> selector = factory.makeObjectColumnSelector(arrayMetric);
      final ValueDesc elementType = ValueDesc.subElementOf(indexed, ValueDesc.UNKNOWN);
      return new ObjectColumnSelector()
      {
        @Override
        public Object get()
        {
          List list = selector.get();
          return access < list.size() ? list.get(access) : null;
        }

        @Override
        public ValueDesc type()
        {
          return elementType;
        }
      };
    }
    return null;
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
  public DimensionSelector asDimension(String dimension, ColumnSelectorFactory factory)
  {
    ObjectColumnSelector selector = asMetric(dimension, factory);
    if (selector == null || !ValueDesc.isString(selector.type())) {
      throw new UnsupportedOperationException(dimension + " cannot be used as dimension");
    }
    return VirtualColumns.toDimensionSelector(selector);
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new ArrayVirtualColumn(arrayMetric, outputName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] valueMet = StringUtils.toUtf8WithNullToEmpty(arrayMetric);
    byte[] output = StringUtils.toUtf8(outputName);

    return ByteBuffer.allocate(2 + valueMet.length + output.length)
                     .put(VC_TYPE_ID)
                     .put(valueMet).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(output)
                     .array();
  }

  @JsonProperty
  public String getArrayMetric()
  {
    return arrayMetric;
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
    if (!(o instanceof ArrayVirtualColumn)) {
      return false;
    }

    ArrayVirtualColumn that = (ArrayVirtualColumn) o;

    if (!arrayMetric.equals(that.arrayMetric)) {
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
    int result = arrayMetric.hashCode();
    result = 31 * result + outputName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "ArrayVirtualColumn{" +
           "arrayMetric='" + arrayMetric + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
