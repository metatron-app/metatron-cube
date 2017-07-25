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

package io.druid.query;

import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
public class RowResolver implements TypeResolver
{
  public static Class<?> toClass(String typeName)
  {
    ValueDesc valueDesc = ValueDesc.of(typeName);
    switch (valueDesc.type()) {
      case STRING:
        return String.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case LONG:
        return Long.class;
    }
    switch (typeName.toLowerCase()) {
      case ValueDesc.MAP_TYPE:
        return Map.class;
      case ValueDesc.LIST_TYPE:
        return List.class;
      case ValueDesc.DATETIME_TYPE:
        return DateTime.class;
      case ValueDesc.UNKNOWN_TYPE:
        return Object.class;
    }

    if (typeName.equals(ValueDesc.INDEXED_ID_TYPE) || typeName.startsWith(ValueDesc.INDEXED_ID_PREFIX)) {
      return IndexedInts.WithLookup.class;
    }

    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
    if (serde != null) {
      return serde.getObjectStrategy().getClazz();
    }
    return Object.class;
  }

  public static ValueDesc toValueType(Class<?> clazz)
  {
    if (clazz == null || clazz == Object.class) {
      return ValueDesc.UNKNOWN;
    } else if (clazz == String.class) {
      return ValueDesc.STRING;
    } else if (clazz == Float.class || clazz == Float.TYPE) {
      return ValueDesc.FLOAT;
    } else if (clazz == Double.class || clazz == Double.TYPE) {
      return ValueDesc.DOUBLE;
    } else if (clazz == Long.class || clazz == Long.TYPE) {
      return ValueDesc.LONG;
    } else if (Map.class.isAssignableFrom(clazz)) {
      return ValueDesc.MAP;
    } else if (List.class.isAssignableFrom(clazz)) {
      return ValueDesc.LIST;
    } else if (IndexedInts.WithLookup.class.isAssignableFrom(clazz)) {
      return ValueDesc.INDEXED_ID;
    } else if (clazz.isArray()) {
      return ValueDesc.ofArray(toValueType(clazz.getComponentType()));
    }
    // cannot make multi-valued type from class

    String typeName = ComplexMetrics.getTypeNameForClass(clazz);
    if (typeName != null) {
      return ValueDesc.of(typeName);
    }
    return ValueDesc.UNKNOWN;
  }

  private final Map<String, ValueDesc> columnTypes = Maps.newHashMap();
  private final Map<String, Pair<VirtualColumn, ValueDesc>> virtualColumnTypes = Maps.newHashMap();
  private final VirtualColumns virtualColumns;

  public RowResolver(StorageAdapter adapter, VirtualColumns virtualColumns)
  {
    for (String dimension : adapter.getAvailableDimensions()) {
      columnTypes.put(dimension, toColumnType(adapter, dimension));
    }
    for (String metric : adapter.getAvailableMetrics()) {
      columnTypes.put(metric, toColumnType(adapter, metric));
    }
    this.virtualColumns = virtualColumns;
  }

  private ValueDesc toColumnType(StorageAdapter adapter, String dimension)
  {
    ColumnCapabilities capabilities = adapter.getColumnCapabilities(dimension);
    if (capabilities == null) {
      return ValueDesc.UNKNOWN;
    } else if (capabilities.getType() == ValueType.COMPLEX) {
      return ValueDesc.of(adapter.getColumnTypeName(dimension));
    }
    return ValueDesc.of(capabilities.getType());
  }

  @Override
  public ValueDesc resolveColumn(String column)
  {
    return resolveColumn(column, null);
  }

  @Override
  public ValueDesc resolveColumn(String column, ValueDesc defaultType)
  {
    ValueDesc columnType = columnTypes.get(column);
    if (columnType != null) {
      return columnType;
    }
    Pair<VirtualColumn, ValueDesc> vcResolved = virtualColumnTypes.get(column);
    if (vcResolved != null) {
      return vcResolved.rhs;
    }
    VirtualColumn vc = virtualColumns.getVirtualColumn(column);
    if (vc != null) {
      ValueDesc valueType = vc.resolveType(column, this);
      if (valueType != null) {
        virtualColumnTypes.put(column, Pair.of(vc, valueType));
        return valueType;
      }
    }
    return defaultType;
  }
}
