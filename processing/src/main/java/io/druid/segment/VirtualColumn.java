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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.common.Cacheable;
import io.druid.data.TypeResolver;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.StructMetricSerde;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = ExprVirtualColumn.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "map", value = MapVirtualColumn.class),
    @JsonSubTypes.Type(name = "expr", value = ExprVirtualColumn.class),
    @JsonSubTypes.Type(name = "index", value = KeyIndexedVirtualColumn.class),
    @JsonSubTypes.Type(name = "lateral", value = LateralViewVirtualColumn.class),
    @JsonSubTypes.Type(name = "array", value = ArrayVirtualColumn.class),
    @JsonSubTypes.Type(name = "struct", value = StructVirtualColumn.class),
    @JsonSubTypes.Type(name = "dateTime", value = DateTimeVirtualColumn.class),
    @JsonSubTypes.Type(name = "dimensionSpec", value = DimensionSpecVirtualColumn.class),
    @JsonSubTypes.Type(name = "$attachment", value = AttachmentVirtualColumn.class)
})
public interface VirtualColumn extends Cacheable
{
  String getOutputName();

  default ValueDesc resolveType(TypeResolver resolver)
  {
    return resolveType(getOutputName(), resolver);
  }

  ValueDesc resolveType(String column, TypeResolver resolver);

  ObjectColumnSelector asMetric(String dimension, ColumnSelectorFactory factory);

  default FloatColumnSelector asFloatMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asFloat(asMetric(dimension, factory));
  }

  default DoubleColumnSelector asDoubleMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asDouble(asMetric(dimension, factory));
  }

  default LongColumnSelector asLongMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asLong(asMetric(dimension, factory));
  }

  default DimensionSelector asDimension(DimensionSpec dimension, ColumnSelectorFactory factory)
  {
    return VirtualColumns.toDimensionSelector(asMetric(dimension.getDimension(), factory), dimension.getExtractionFn());
  }

  VirtualColumn duplicate();

  interface IndexProvider extends VirtualColumn
  {
    String sourceColumn();

    Set<String> targetColumns();

    default ColumnSelectorFactory override(ColumnSelectorFactory factory)
    {
      return VirtualColumns.wrap(IndexProvider.this, factory, getOutputName(), targetColumns());
    }
  }

  static ValueDesc nested(ValueDesc columnType, String expression)
  {
    if (columnType.isMap()) {
      String[] description = TypeUtils.splitDescriptiveType(columnType);
      if (expression.equals(Row.MAP_KEY)) {
        return description == null ? ValueDesc.STRING : ValueDesc.of(description[1]);
      }
      if (expression.equals(Row.MAP_VALUE)) {
        return description == null ? null : ValueDesc.of(description[2]);
      }
      return null;
    }
    if (columnType.isStruct()) {
      StructMetricSerde serde = (StructMetricSerde) ComplexMetrics.getSerdeForType(columnType);
      int ix = -1;
      int fx = serde.indexOf(expression);
      if (fx < 0) {
        for (ix = expression.indexOf('.'); ix > 0; ix = expression.indexOf('.', ix + 1)) {
          fx = serde.indexOf(expression.substring(0, ix));
          if (fx > 0) {
            break;
          }
        }
      }
      return fx < 0 ? null : ix < 0 ? serde.type(fx) : nested(serde.type(fx), expression.substring(ix + 1));
    }
    if (columnType.isArray()) {
      if (columnType.isPrimitiveArray()) {
        return columnType.subElement(ValueDesc.UNKNOWN);
      }
      String[] description = TypeUtils.splitDescriptiveType(columnType);
      if (description == null) {
        return ValueDesc.UNKNOWN;
      }
      int ix = expression.indexOf('.');
      final Integer access = Ints.tryParse(ix < 0 ? expression : expression.substring(0, ix));
      if (access == null || access < 0) {
        return null;
      }
      ValueDesc elementType = ValueDesc.of(description[1]);
      return ix < 0 ? elementType : nested(elementType, expression.substring(ix + 1));
    }
    return null;
  }

  // for incremental index
  static ObjectColumnSelector nested(ObjectColumnSelector selector, String expression)
  {
    ValueDesc type = selector.type();
    if (type.isMap()) {
      if (Row.MAP_KEY.equals(expression)) {
        String[] description = TypeUtils.splitDescriptiveType(type);
        return ObjectColumnSelector.typed(
            description == null ? ValueDesc.STRING : ValueDesc.of(description[1]),
            () -> {
              Map v = (Map) selector.get();
              return v == null ? null : Lists.newArrayList(v.keySet());
            }
        );
      }
      if (Row.MAP_VALUE.equals(expression)) {
        String[] description = TypeUtils.splitDescriptiveType(type);
        return ObjectColumnSelector.typed(
            description == null ? ValueDesc.UNKNOWN : ValueDesc.of(description[2]),
            () -> {
              Map v = (Map) selector.get();
              return v == null ? null : Lists.newArrayList(v.values());
            }
        );
      }
      return ColumnSelectors.NULL_UNKNOWN;
    }
    if (type.isStruct()) {
      StructMetricSerde serde = (StructMetricSerde) ComplexMetrics.getSerdeForType(type);
      int ix = -1;
      int index = serde.indexOf(expression);
      if (index < 0) {
        for (ix = expression.indexOf('.'); ix > 0; ix = expression.indexOf('.', ix + 1)) {
          index = serde.indexOf(expression.substring(0, ix));
          if (index > 0) {
            break;
          }
        }
      }
      if (index < 0) {
        return ColumnSelectors.NULL_UNKNOWN;
      }
      final int vindex = index;
      final String fieldName = ix < 0 ? expression : expression.substring(0, ix);
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
      return ix < 0 ? nested : nested(nested, expression.substring(ix + 1));
    }
    if (type.isArray()) {
      int ix = expression.indexOf('.');
      Integer access = Ints.tryParse(ix < 0 ? expression : expression.substring(0, ix));
      if (access == null) {
        return ColumnSelectors.NULL_UNKNOWN;
      }
      Supplier<?> supplier = () -> {
        List list = (List) selector.get();
        return access < list.size() ? list.get(access) : null;
      };
      ValueDesc elementType;
      if (type.isPrimitiveArray()) {
        elementType = type.subElement(ValueDesc.UNKNOWN);
      } else {
        String[] description = TypeUtils.splitDescriptiveType(type);
        elementType = description == null ? ValueDesc.UNKNOWN : ValueDesc.of(description[1]);
      }
      ObjectColumnSelector nested = ObjectColumnSelector.typed(elementType, supplier);
      return ix < 0 ? nested : nested(nested, expression.substring(ix + 1));
    }
    return ColumnSelectors.NULL_UNKNOWN;
  }
}
