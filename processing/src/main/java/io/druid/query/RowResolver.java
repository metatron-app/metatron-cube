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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.Pair;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.select.StreamQuery;
import io.druid.segment.SchemaProvider;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.data.IndexedID;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
public class RowResolver implements TypeResolver
{
  public static Supplier<RowResolver> supplier(final List<Segment> segments, final Query query)
  {
    return Suppliers.memoize(() -> of(segments, BaseQuery.getVirtualColumns(query)));
  }

  public static RowResolver of(List<Segment> segments, List<VirtualColumn> virtualColumns)
  {
    Preconditions.checkArgument(!segments.isEmpty());
    RowSignature signature = segments.get(0).asSignature(true);
    for (int i = 1; i < segments.size(); i++) {
      signature = signature.merge(segments.get(i).asSignature(true));
    }
    return of(signature, virtualColumns);
  }

  public static RowResolver of(SchemaProvider segment, List<VirtualColumn> virtualColumns)
  {
    return of(segment.asSignature(true), virtualColumns);
  }

  public static RowResolver of(RowSignature schema, List<VirtualColumn> virtualColumns)
  {
    return new RowResolver(schema, virtualColumns);
  }

  public static Class<?> toClass(ValueDesc valueDesc)
  {
    Class clazz = valueDesc.asClass();
    if (clazz != null && clazz != Object.class) {
      return clazz;
    }
    if (ValueDesc.isIndexedId(valueDesc)) {
      return IndexedID.class;
    }
    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(valueDesc.typeName());
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
      IndexedID lookup = (IndexedID) object;
      return lookup == null ? ValueDesc.INDEXED_ID : ValueDesc.ofIndexedId(lookup.elementType());
    } else if (clazz.isArray()) {
      return ValueDesc.ofArray(toValueType(clazz.getComponentType(), ValueDesc.UNKNOWN));
    }
    // cannot make multi-valued type from class

    ValueDesc typeName = ComplexMetrics.getTypeNameForClass(clazz);
    if (typeName != null) {
      return typeName;
    }
    return ValueDesc.UNKNOWN;
  }

  private final RowSignature schema;
  private final VirtualColumns virtualColumns;

  private final Map<String, Pair<VirtualColumn, ValueDesc>> virtualColumnTypes = Maps.newConcurrentMap();

  private RowResolver(RowSignature schema, List<VirtualColumn> virtualColumns)
  {
    this.schema = schema;
    this.virtualColumns = VirtualColumns.valueOf(virtualColumns, schema);
  }

  private RowResolver(RowSignature schema, VirtualColumns virtualColumns)
  {
    this.schema = schema;
    this.virtualColumns = virtualColumns;
  }

  @VisibleForTesting
  public RowResolver(Map<String, ValueDesc> columnTypes, VirtualColumns virtualColumns)
  {
    this.schema = Schema.of(columnTypes);
    this.virtualColumns = virtualColumns;
  }

  public List<String> getColumnNames()
  {
    return schema.getColumnNames();
  }

  public List<ValueDesc> getColumnTypes()
  {
    return schema.getColumnTypes();
  }

  public int size()
  {
    return schema.size();
  }

  public List<String> getDimensionNames()
  {
    return schema.getDimensionNames();
  }

  public List<String> getMetricNames()
  {
    return schema.getMetricNames();
  }

  public Map<String, AggregatorFactory> getAggregators()
  {
    return schema instanceof Schema ? ((Schema) schema).getAggregators() : Collections.emptyMap();
  }

  public Iterable<String> getAllColumnNames(List<VirtualColumn> vcs)
  {
    if (vcs.isEmpty()) {
      return schema.getColumnNames();
    }
    Set<String> names = Sets.newLinkedHashSet();
    vcs.stream()
       .filter(vc -> virtualColumns.getVirtualColumn(vc.getOutputName()) != null)
       .forEach(vc -> names.add(vc.getOutputName()));
    names.addAll(schema.getColumnNames()); // override
    return names;
  }

  public VirtualColumn getVirtualColumn(String columnName)
  {
    return virtualColumns.getVirtualColumn(columnName);
  }

  public List<VirtualColumn> getVirtualColumns()
  {
    return ImmutableList.copyOf(virtualColumns.iterator());
  }

  @Override
  public ValueDesc resolve(String column)
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
      if (valueType != null && !valueType.isUnknown()) {
        virtualColumnTypes.put(column, Pair.of(vc, valueType));
        return valueType;
      }
      return ValueDesc.UNKNOWN;
    }
    return null;
  }

  public RowResolver resolve(StreamQuery query)
  {
    List<String> names = query.getColumns();
    List<ValueDesc> types = names.stream()
                                 .map(c -> resolve(c, ValueDesc.UNKNOWN))
                                 .map(desc -> desc.unwrapDimension())
                                 .collect(Collectors.toList());
    return new RowResolver(RowSignature.of(names, types), virtualColumns);
  }
}
