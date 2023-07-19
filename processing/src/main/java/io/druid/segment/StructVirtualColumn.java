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
import com.google.common.collect.Lists;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ComplexColumnSelector.StructColumnSelector;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.StructMetricSerde;

import java.util.List;
import java.util.Map;
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
    ValueDesc columnType = types.resolve(columnName, ValueDesc.UNKNOWN);
    Preconditions.checkArgument(columnType.isStruct(), "%s is not struct type", columnName);
    if (column.equals(outputName)) {
      return columnType;
    }
    return nested(columnType, column.substring(columnName.length() + 1));
  }

  private ValueDesc nested(ValueDesc columnType, String field)
  {
    if (columnType.isMap()) {
      String[] description = TypeUtils.splitDescriptiveType(columnType.typeName());
      if (field.equals(Row.MAP_KEY)) {
        return description == null ? ValueDesc.STRING : ValueDesc.of(description[1]);
      }
      if (field.equals(Row.MAP_VALUE)) {
        return description == null ? ValueDesc.UNKNOWN : ValueDesc.of(description[2]);
      }
      return ValueDesc.UNKNOWN;
    }
    StructMetricSerde serde = (StructMetricSerde) ComplexMetrics.getSerdeForType(columnType);
    int ix = -1;
    int index = serde.indexOf(field);
    if (index < 0) {
      for (ix = field.indexOf('.'); ix > 0; ix = field.indexOf('.', ix + 1)) {
        index = serde.indexOf(field.substring(0, ix));
        if (index > 0) {
          break;
        }
      }
    }
    if (index < 0) {
      return ValueDesc.UNKNOWN;
    }
    return ix > 0 ? nested(serde.type(index), field.substring(ix + 1)) : serde.type(index);
  }

  @Override
  public ObjectColumnSelector asMetric(String dimension, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(dimension.startsWith(outputName));
    final ValueDesc columnType = factory.resolve(columnName);
    if (columnType == null) {
      return ColumnSelectors.nullObjectSelector(ValueDesc.UNKNOWN);
    }
    Preconditions.checkArgument(columnType.isStruct(), "%s is not struct type (was %s)", columnName, columnType);

    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(columnName);
    if (dimension.equals(outputName)) {
      return selector;
    }
    final String fieldName = dimension.substring(columnName.length() + 1);
    if (selector instanceof StructColumnSelector) {
      return ((StructColumnSelector) selector).fieldSelector(fieldName);
    }
    return nested(selector, fieldName);
  }

  // for incremental index -_-;;;;
  private ObjectColumnSelector nested(ObjectColumnSelector selector, String field)
  {
    ValueDesc type = selector.type();
    if (type.isMap()) {
      if (Row.MAP_KEY.equals(field)) {
        String[] description = TypeUtils.splitDescriptiveType(type.typeName());
        return ObjectColumnSelector.typed(
            description == null ? ValueDesc.STRING : ValueDesc.of(description[1]),
            () -> {
              Map v = (Map) selector.get();
              return v == null ? null : Lists.newArrayList(v.keySet());
            }
        );
      }
      if (Row.MAP_VALUE.equals(field)) {
        String[] description = TypeUtils.splitDescriptiveType(type.typeName());
        return ObjectColumnSelector.typed(
            description == null ? ValueDesc.UNKNOWN : ValueDesc.of(description[2]),
            () -> {
              Map v = (Map) selector.get();
              return v == null ? null : Lists.newArrayList(v.values());
            }
        );
      }
      return ColumnSelectors.nullObjectSelector(ValueDesc.UNKNOWN);
    }
    StructMetricSerde serde = (StructMetricSerde) ComplexMetrics.getSerdeForType(type);
    int ix = -1;
    int index = serde.indexOf(field);
    if (index < 0) {
      for (ix = field.indexOf('.'); ix > 0; ix = field.indexOf('.', ix + 1)) {
        index = serde.indexOf(field.substring(0, ix));
        if (index > 0) {
          break;
        }
      }
    }
    if (index < 0) {
      return ColumnSelectors.nullObjectSelector(ValueDesc.UNKNOWN);
    }
    final int vindex = index;
    final String fieldName = ix < 0 ? field : field.substring(0, ix);
    final ValueDesc fieldType = serde.type(index, f -> f.isString() ? ValueDesc.MV_STRING : f);
    final ObjectColumnSelector nested = new ObjectColumnSelector.Typed(fieldType)
    {
      @Override
      public Object get()
      {
        final Object o = selector.get();
        if (o == null) {
          return null;
        } else if (o instanceof List) {
          return ((List) o).get(vindex);
        } else if (o instanceof Object[]) {
          return ((Object[]) o)[vindex];
        } else if (o instanceof Map) {
          return ((Map) o).get(fieldName);
        }
        return null;
      }
    };
    return ix > 0 ? nested(nested, field.substring(ix + 1)) : nested;
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
