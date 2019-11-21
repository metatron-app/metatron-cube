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

package io.druid.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.java.util.common.Pair;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.select.Schema;
import io.druid.segment.SchemaProvider;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedID;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class RowResolver implements TypeResolver, Function<String, ValueDesc>
{
  public static Supplier<RowResolver> supplier(final List<Segment> segments, final Query query)
  {
    return Suppliers.memoize(
        new Supplier<RowResolver>()
        {
          @Override
          public RowResolver get()
          {
            return of(segments, BaseQuery.getVirtualColumns(query));
          }
        }
    );
  }

  public static RowResolver of(List<Segment> segments, List<VirtualColumn> virtualColumns)
  {
    Preconditions.checkArgument(!segments.isEmpty());
    Schema schema = segments.get(0).asSchema(true);
    for (int i = 1; i < segments.size(); i++) {
      schema = schema.merge(segments.get(i).asSchema(true));
    }
    return of(schema, virtualColumns);
  }

  public static RowResolver of(SchemaProvider segment, List<VirtualColumn> virtualColumns)
  {
    return of(segment.asSchema(true), virtualColumns);
  }

  public static RowResolver of(Schema schema, List<VirtualColumn> virtualColumns)
  {
    return new RowResolver(schema, virtualColumns);
  }

  public static Class<?> toClass(ValueDesc valueDesc)
  {
    switch (valueDesc.type()) {
      case BOOLEAN:
        return Boolean.class;
      case STRING:
        return String.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case LONG:
        return Long.class;
      case DATETIME:
        return DateTime.class;
    }
    String typeName = valueDesc.typeName();
    switch (typeName.toLowerCase()) {
      case ValueDesc.MAP_TYPE:
        return Map.class;
      case ValueDesc.LIST_TYPE:
        return List.class;
      case ValueDesc.UNKNOWN_TYPE:
        return Object.class;
    }

    if (typeName.startsWith(ValueDesc.INDEXED_ID_PREFIX)) {
      return IndexedID.class;
    }

    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
    if (serde != null) {
      return serde.getObjectStrategy().getClazz();
    }
    return Object.class;
  }

  public static ValueDesc toValueType(Object obj)
  {
    return obj == null ? ValueDesc.UNKNOWN : toValueType(obj.getClass(), obj);
  }

  private static ValueDesc toValueType(Class clazz, Object object)
  {
    if (clazz == Object.class) {
      return ValueDesc.UNKNOWN;
    } else if (clazz == String.class) {
      return ValueDesc.STRING;
    } else if (clazz == Boolean.class || clazz == Boolean.TYPE) {
      return ValueDesc.BOOLEAN;
    } else if (clazz == Float.class || clazz == Float.TYPE) {
      return ValueDesc.FLOAT;
    } else if (clazz == Double.class || clazz == Double.TYPE) {
      return ValueDesc.DOUBLE;
    } else if (clazz == Long.class || clazz == Long.TYPE || clazz == Integer.class || clazz == Integer.TYPE) {
      return ValueDesc.LONG;
    } else if (clazz == DateTime.class) {
      return ValueDesc.DATETIME;
    } else if (clazz == BigDecimal.class) {
      return object == null ? ValueDesc.DECIMAL : ValueDesc.ofDecimal((BigDecimal) object);
    } else if (Map.class.isAssignableFrom(clazz)) {
      return ValueDesc.MAP;
    } else if (List.class.isAssignableFrom(clazz)) {
      return ValueDesc.STRUCT;
    } else if (IndexedID.class.isAssignableFrom(clazz)) {
      IndexedID lookup = (IndexedID)object;
      return lookup == null ? ValueDesc.INDEXED_ID : ValueDesc.ofIndexedId(lookup.elementType());
    } else if (clazz.isArray()) {
      return ValueDesc.ofArray(toValueType(clazz.getComponentType(), ValueDesc.UNKNOWN));
    }
    // cannot make multi-valued type from class

    String typeName = ComplexMetrics.getTypeNameForClass(clazz);
    if (typeName != null) {
      return ValueDesc.of(typeName);
    }
    return ValueDesc.UNKNOWN;
  }

  private final Schema schema;
  private final VirtualColumns virtualColumns;

  private final Map<String, Pair<VirtualColumn, ValueDesc>> virtualColumnTypes = Maps.newConcurrentMap();

  private RowResolver(Schema schema, List<VirtualColumn> virtualColumns)
  {
    this.schema = schema;
    this.virtualColumns = VirtualColumns.valueOf(virtualColumns, schema);
  }

  @VisibleForTesting
  public RowResolver(Map<String, ValueDesc> columnTypes, VirtualColumns virtualColumns)
  {
    this.schema = Schema.of(columnTypes);
    this.virtualColumns = virtualColumns;
  }

  public List<String> getDimensionNames()
  {
    return schema.getDimensionNames();
  }

  public List<String> getDimensionNamesExceptTime()
  {
    final List<String> dimensions = schema.getDimensionNames();
    final int index = dimensions.indexOf(Row.TIME_COLUMN_NAME);
    if (index >= 0) {
      dimensions.remove(index);
    }
    return dimensions;
  }

  public List<ValueDesc> getDimensionTypes()
  {
    return schema.getDimensionTypes();
  }

  public List<String> getMetricNames()
  {
    return schema.getMetricNames();
  }

  public List<ValueDesc> getMetricTypes()
  {
    return schema.getMetricTypes();
  }

  public List<String> getColumnNames()
  {
    return schema.getColumnNames();
  }

  public boolean isDimension(String columnName)
  {
    return schema.getDimensionNames().indexOf(columnName) >= 0;
  }

  public boolean isMetric(String columnName)
  {
    return schema.getMetricNames().indexOf(columnName) >= 0;
  }

  public Map<String, AggregatorFactory> getAggregators()
  {
    return schema.getAggregators();
  }

  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return schema.getColumnCapability(column);
  }

  public Map<String, String> getDescriptor(String column)
  {
    return schema.getColumnDescriptor(column);
  }

  public VirtualColumn getVirtualColumn(String columnName)
  {
    return virtualColumns.getVirtualColumn(columnName);
  }

  @Override
  public ValueDesc apply(String input)
  {
    return resolve(input);
  }

  @Override
  public ValueDesc resolve(String column)
  {
    return resolve(column, null);
  }

  @Override
  public ValueDesc resolve(String column, ValueDesc defaultType)
  {
    ValueDesc resolved = schema.resolve(column);
    if (resolved != null) {
      return resolved;
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

  public VirtualColumn resolveVC(String column)
  {
    if (resolve(column) == null) {
      return null;
    }
    Pair<VirtualColumn, ValueDesc> resolved = virtualColumnTypes.get(column);
    if (resolved != null && resolved.lhs != null) {
      return resolved.lhs;
    }
    return null;
  }

  public Iterable<String> getAllColumnNames()
  {
    Set<String> names = Sets.newLinkedHashSet(virtualColumns.getVirtualColumnNames());
    names.addAll(schema.getColumnNames()); // override
    return names;
  }
}
