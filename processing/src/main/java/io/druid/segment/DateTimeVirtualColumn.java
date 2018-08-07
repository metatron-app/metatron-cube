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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilterCacheHelper;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;

/**
 */
public class DateTimeVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x06;

  private final String columnName;
  private final String outputName;

  @JsonCreator
  public DateTimeVirtualColumn(
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
    if (column.equals(columnName)) {
      return types.resolve(columnName);
    }
    return ValueDesc.LONG;
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(column.startsWith(outputName));
    final int index = column.indexOf('.', outputName.length());
    if (index < 0) {
      return factory.makeObjectColumnSelector(columnName);
    }
    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(columnName.substring(index));
    final String function = columnName.substring(index + 1).toUpperCase();
    switch (function) {
      case "YEAR":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getYear();
          }
        });
      case "MONTHOFYEAR":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getMonthOfYear();
          }
        });
      case "DAYOFYEAR":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getDayOfYear();
          }
        });
      case "DAYOFMONTH":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getDayOfMonth();
          }
        });
      case "DAYOFWEEK":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getDayOfWeek();
          }
        });
      case "HOUROFDAY":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getHourOfDay();
          }
        });
      case "MINUTEOFDAY":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getMinuteOfDay();
          }
        });
      case "MINUTEOFHOUR":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getMinuteOfHour();
          }
        });
      case "SECONDOFDAY":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getSecondOfDay();
          }
        });
      case "SECONDOFMINUTE":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getSecondOfMinute();
          }
        });
      case "MILLISOFDAY":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getMillisOfDay();
          }
        });
      case "MILLISOFSECOND":
        return ColumnSelectors.map(selector, ValueDesc.LONG, new Function()
        {
          @Override
          public Long apply(Object input)
          {
            return (long) ((DateTime) ValueDesc.DATETIME.cast(input)).getMillisOfSecond();
          }
        });
      default:
        throw new IllegalArgumentException("invalid function " + function);
    }
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
  public DimensionSelector asDimension(String dimension, ExtractionFn extractionFn, ColumnSelectorFactory factory)
  {
    ObjectColumnSelector selector = asMetric(dimension, factory);
    if (selector == null || !ValueDesc.isString(selector.type())) {
      throw new UnsupportedOperationException(dimension + " cannot be used as dimension");
    }
    return VirtualColumns.toDimensionSelector(selector, extractionFn);
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new DateTimeVirtualColumn(columnName, outputName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] columnNameBytes = StringUtils.toUtf8WithNullToEmpty(columnName);
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
    if (!(o instanceof DateTimeVirtualColumn)) {
      return false;
    }

    DateTimeVirtualColumn that = (DateTimeVirtualColumn) o;

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
    return "DateTimeVirtualColumn{" +
           "columnName='" + columnName + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
