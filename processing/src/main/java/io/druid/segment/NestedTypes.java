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

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.segment.ObjectColumnSelector.ListBacked;
import io.druid.segment.serde.StructMetricSerde;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

// resolving logics for incremental index
public class NestedTypes
{
  public static ValueDesc toRuntimeType(ValueDesc columnType)
  {
    return rewrite(columnType, t -> t.isEnum() || t.isTag() ? ValueDesc.MV_STRING : t);   // hacky..
  }

  public static ValueDesc rewrite(ValueDesc columnType, Function<ValueDesc, ValueDesc> rewriter)
  {
    if (columnType.isMap()) {
      String[] description = TypeUtils.splitDescriptiveType(columnType);
      ValueDesc k1 = description == null ? ValueDesc.STRING : ValueDesc.of(description[1]);
      ValueDesc v1 = description == null ? ValueDesc.UNKNOWN : ValueDesc.of(description[2]);
      ValueDesc k2 = rewrite(k1, rewriter);
      ValueDesc v2 = rewrite(v1, rewriter);
      if (k1 != k2 || v1 != v2) {
        columnType = ValueDesc.ofMap(k2, v2);
      }
    } else if (columnType.isStruct()) {
      boolean changed = false;
      List<ValueDesc> types = Lists.newArrayList();
      StructMetricSerde struct = StructMetricSerde.parse(columnType);
      for (ValueDesc type : struct.getFieldTypes()) {
        ValueDesc rewritten = rewrite(type, rewriter);
        types.add(rewritten);
        changed |= type != rewritten;
      }
      if (changed) {
        columnType = ValueDesc.ofStruct(struct.getFieldNames(), types.toArray(new ValueDesc[0]));
      }
    } else if (columnType.isArray()) {
      ValueDesc elementType = columnType.unwrapArray();
      ValueDesc rewritten = rewrite(elementType, rewriter);
      if (elementType != rewritten) {
        columnType = ValueDesc.ofArray(rewritten);
      }
    }
    return rewriter.apply(columnType);
  }

  public static ValueDesc resolve(ValueDesc columnType, String expression)
  {
    if (columnType.isMap()) {
      String[] description = TypeUtils.splitDescriptiveType(columnType);
      if (expression.equals(Row.MAP_KEY)) {
        return description == null ? ValueDesc.STRING : ValueDesc.of(description[1]);
      }
      return description == null ? ValueDesc.UNKNOWN : ValueDesc.of(description[2]);
    }
    if (columnType.isStruct()) {
      StructMetricSerde serde = StructMetricSerde.parse(columnType);
      if (expression.equals(Row.MAP_KEY)) {
        return ValueDesc.STRING_ARRAY;
      }
      int ix = -1;
      int fx = serde.indexOf(expression);
      if (fx < 0) {
        for (ix = expression.indexOf('.'); ix > 0; ix = expression.indexOf('.', ix + 1)) {
          fx = serde.indexOf(expression.substring(0, ix));
          if (fx >= 0) {
            break;
          }
        }
      }
      return fx < 0 ? null : ix < 0 ? serde.type(fx) : resolve(serde.type(fx), expression.substring(ix + 1));
    }
    if (columnType.isArray()) {
      ValueDesc elementType = columnType.unwrapArray();
      int ix = expression.indexOf('.');
      Integer access = Ints.tryParse(ix < 0 ? expression : expression.substring(0, ix));
      if (access != null && access >= 0) {
        // element access
        return ix < 0 ? elementType : resolve(elementType, expression.substring(ix + 1));
      }
      if (elementType.isStruct()) {
        ValueDesc fieldType = resolve(elementType, expression);
        if (fieldType != null) {
          return ValueDesc.ofArray(fieldType);
        }
      } else if (elementType.isArray()) {
        ValueDesc fieldType = resolve(elementType.unwrapArray(), expression);
        if (fieldType != null) {
          return ValueDesc.ofArray(fieldType);
        }
      }
      return null;
    }
    return null;
  }

  public static ObjectColumnSelector resolve(ObjectColumnSelector selector, String expression)
  {
    ValueDesc type = selector.type();
    if (type.isMap()) {
      return resolveMap(selector, type, expression);
    }
    if (type.isStruct()) {
      return resolveStruct(selector, type, expression, false);
    }
    if (type.isArray()) {
      return resolveArray(selector, type, expression);
    }
    return ColumnSelectors.NULL_UNKNOWN;
  }

  private static ObjectColumnSelector resolveMap(ObjectColumnSelector selector, ValueDesc type, String expression)
  {
    String[] description = TypeUtils.splitDescriptiveType(type);
    if (Row.MAP_KEY.equals(expression)) {
      return ObjectColumnSelector.typed(
          description == null ? ValueDesc.STRING_ARRAY : ValueDesc.ofArray(description[1]),
          () -> {
            Map v = (Map) selector.get();
            return v == null ? null : Lists.newArrayList(v.keySet());
          }
      );
    }
    if (Row.MAP_VALUE.equals(expression)) {
      return ObjectColumnSelector.typed(
          description == null ? ValueDesc.ARRAY : ValueDesc.ofArray(description[2]),
          () -> {
            Map v = (Map) selector.get();
            return v == null ? null : Lists.newArrayList(v.values());
          }
      );
    }
    return ObjectColumnSelector.typed(
        description == null ? ValueDesc.UNKNOWN : ValueDesc.of(description[2]),
        () -> {
          Map v = (Map) selector.get();
          return v == null ? null : v.get(expression);
        }
    );
  }

  private static ObjectColumnSelector resolveStruct(
      ObjectColumnSelector selector,
      ValueDesc type,
      String expression,
      boolean collect
  )
  {
    StructMetricSerde serde = StructMetricSerde.parse(type);
    if (Row.MAP_KEY.equals(expression)) {
      List<String> fieldNames = Arrays.asList(serde.getFieldNames());
      return ObjectColumnSelector.typed(ValueDesc.STRING_ARRAY, () -> fieldNames);
    }
    int ix = -1;
    int index = serde.indexOf(expression);
    if (index < 0) {
      for (ix = expression.indexOf('.'); ix > 0; ix = expression.indexOf('.', ix + 1)) {
        index = serde.indexOf(expression.substring(0, ix));
        if (index >= 0) {
          break;
        }
      }
    }
    if (index < 0) {
      return ColumnSelectors.NULL_UNKNOWN;
    }
    final int vindex = index;
    final String fieldName = ix < 0 ? expression : expression.substring(0, ix);
    final ValueDesc fieldType = serde.type(index, f -> f.isDimension() ? ValueDesc.MV_STRING : f);

    final ObjectColumnSelector nested;
    if (collect) {
      nested = ObjectColumnSelector.typed(fieldName, ValueDesc.ofArray(fieldType), () -> {
        final List x = (List) selector.get();   // array of struct
        final Object[] ret = new Object[x.size()];
        for (int i = 0; i < ret.length; i++) {
          ret[i] = ((List) x.get(i)).get(vindex);
        }
        return Arrays.asList(ret);
      });
    } else if (selector instanceof ListBacked) {
      nested = ObjectColumnSelector.typed(fieldName, fieldType, () -> ((ListBacked) selector).get(vindex));
    } else {
      nested = ObjectColumnSelector.typed(fieldName, fieldType, () -> {
        final Object o = selector.get();
        if (o instanceof List) {
          return ((List) o).get(vindex);
        } else if (o instanceof Object[]) {
          return ((Object[]) o)[vindex];
        }
        return null;
      });
    }
    return ix < 0 ? nested : resolve(nested, expression.substring(ix + 1));
  }

  private static ObjectColumnSelector resolveArray(ObjectColumnSelector selector, ValueDesc type, String expression)
  {
    ValueDesc element = type.unwrapArray();
    int ix = expression.indexOf('.');
    Integer access = Ints.tryParse(ix < 0 ? expression : expression.substring(0, ix));
    if (access != null) {
      ObjectColumnSelector nested = ObjectColumnSelector.typed(element, () -> {
        List list = (List) selector.get();
        return access < list.size() ? list.get(access) : null;
      });
      return ix < 0 ? nested : resolve(nested, expression.substring(ix + 1));
    }
    if (element.isStruct()) {
      return resolveStruct(selector, element, expression, true);
    }
    if (element.isArray()) {
      return resolve(ColumnSelectors.concat(element, selector), expression);
    }
    return ColumnSelectors.NULL_UNKNOWN;
  }
}
