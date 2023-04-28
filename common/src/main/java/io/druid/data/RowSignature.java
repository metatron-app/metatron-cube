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

package io.druid.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.IntTagged;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 */
public interface RowSignature extends TypeResolver
{
  @JsonProperty
  List<String> getColumnNames();

  @JsonProperty
  List<ValueDesc> getColumnTypes();

  default int size()
  {
    return getColumnNames().size();
  }

  default String columnName(int index)
  {
    return index < 0 ? null : getColumnNames().get(index);
  }

  default ValueDesc columnType(int index)
  {
    return index < 0 ? null : getColumnTypes().get(index);
  }

  default ValueDesc columnType(String name)
  {
    return columnType(getColumnNames().indexOf(name));
  }

  default Iterable<Pair<String, ValueDesc>> columnAndTypes()
  {
    return GuavaUtils.zip(getColumnNames(), getColumnTypes());
  }

  default Iterable<Pair<String, ValueDesc>> dimensionAndTypes()
  {
    return columnAndTypes(type -> type.isDimension());
  }

  default Iterable<Pair<String, ValueDesc>> metricAndTypes()
  {
    return columnAndTypes(type -> !type.isDimension());
  }

  default List<Pair<String, ValueDesc>> columnAndTypes(Predicate<ValueDesc> predicate)
  {
    List<String> columnNames = getColumnNames();
    List<ValueDesc> columnTypes = getColumnTypes();
    List<Pair<String, ValueDesc>> columnAndTypes = Lists.newArrayList();
    for (int i = 0; i < columnTypes.size(); i++) {
      if (predicate.apply(columnTypes.get(i))) {
        columnAndTypes.add(Pair.of(columnNames.get(i), columnTypes.get(i)));
      }
    }
    return columnAndTypes;
  }

  default Iterable<Pair<String, ValueDesc>> columnAndTypes(Iterable<String> columns)
  {
    return Iterables.transform(columns, c -> Pair.of(c, resolve(c)));
  }

  default Map<String, IntTagged<ValueDesc>> columnToIndexAndType()
  {
    Map<String, IntTagged<ValueDesc>> mapping = Maps.newHashMap();
    for (int i = 0; i < size(); i++) {
      mapping.put(columnName(i), IntTagged.of(i, columnType(i)));
    }
    return mapping;
  }

  default List<String> getDimensionNames()
  {
    return columnName(type -> type != null && type.isDimension());
  }

  default List<String> getMetricNames()
  {
    return columnName(type -> type == null || !type.isDimension());
  }

  default List<String> columnName(Predicate<ValueDesc> predicate)
  {
    List<String> columnNames = getColumnNames();
    List<ValueDesc> columnTypes = getColumnTypes();
    List<String> predicated = Lists.newArrayList();
    for (int i = 0; i < columnTypes.size(); i++) {
      if (predicate.apply(columnTypes.get(i))) {
        predicated.add(columnNames.get(i));
      }
    }
    return predicated;
  }

  default String asTypeString()
  {
    final StringBuilder s = new StringBuilder();
    for (Pair<String, ValueDesc> pair : columnAndTypes()) {
      if (s.length() > 0) {
        s.append(',');
      }
      s.append(pair.lhs).append(':').append(ValueDesc.toTypeString(pair.rhs));
    }
    return s.toString();
  }

  default Map<String, ValueDesc> asTypeMap()
  {
    final Map<String, ValueDesc> map = Maps.newHashMap();
    for (Pair<String, ValueDesc> pair : columnAndTypes()) {
      map.put(pair.lhs, pair.rhs);
    }
    return map;
  }

  default boolean anyType(Predicate<ValueDesc> predicate)
  {
    return Iterables.any(getColumnTypes(), predicate);
  }

  default IntStream indexOf(Predicate<ValueDesc> predicate)
  {
    List<ValueDesc> types = getColumnTypes();
    return IntStream.range(0, size()).filter(x -> predicate.apply(types.get(x)));
  }

  default int indexOf(String column)
  {
    return getColumnNames().indexOf(column);
  }
}
