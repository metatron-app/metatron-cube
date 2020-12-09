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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.Pair;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = RowSignature.Simple.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "simple", value = RowSignature.Simple.class),
    @JsonSubTypes.Type(name = "schema", value = Schema.class)
})
public interface RowSignature extends TypeResolver
{
  // this is needed to be implemented by all post processors, but let's do it step by step
  interface Evolving
  {
    List<String> evolve(List<String> schema);

    RowSignature evolve(Query query, RowSignature schema);
  }

  @JsonProperty
  List<String> getColumnNames();

  @JsonProperty
  List<ValueDesc> getColumnTypes();

  default int size()
  {
    return getColumnNames().size();
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

  default List<String> getDimensionNames()
  {
    return columnName(type -> type.isDimension());
  }

  default List<String> getMetricNames()
  {
    return columnName(type -> !type.isDimension());
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

  // for streaming sub query.. we don't have index
  default RowSignature replaceDimensionToString()
  {
    List<ValueDesc> replaced = Lists.newArrayList(getColumnTypes());
    for (int i = 0; i < replaced.size(); i++) {
      if (ValueDesc.isDimension(replaced.get(i))) {
        replaced.set(i, ValueDesc.STRING);
      }
    }
    return new Simple(getColumnNames(), replaced);
  }

  default RowSignature retain(List<String> columns)
  {
    return new Simple(
        columns, ImmutableList.copyOf(Iterables.transform(columns, name -> resolve(name, ValueDesc.UNKNOWN)))
    );
  }

  default RowSignature concat(RowSignature other)
  {
    return new Simple(
        GuavaUtils.concat(getColumnNames(), other.getColumnNames()),
        GuavaUtils.concat(getColumnTypes(), other.getColumnTypes())
    );
  }

  default RowSignature relay(Query<?> query, boolean finalzed)
  {
    RowSignature signature = this;
    if (BaseQuery.isBrokerSide(query)) {
      signature = Queries.finalize(Queries.bestEffortOf(signature, query, finalzed), query.toLocalQuery(), null);
    } else {
      signature = Queries.bestEffortOf(signature, query, finalzed);
    }
    return Queries.finalize(signature, query, null);
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

  @JsonTypeName("simple")
  class Simple implements RowSignature
  {
    public static RowSignature of(List<String> columnNames, List<ValueDesc> columnTypes)
    {
      return new Simple(columnNames, columnTypes);
    }

    public static RowSignature fromTypeString(String typeString)
    {
      List<String> columnNames = Lists.newArrayList();
      List<ValueDesc> columnTypes = Lists.newArrayList();
      for (String column : StringUtils.split(typeString, ',')) {
        final int index = column.indexOf(':');
        columnNames.add(column.substring(0, index));
        columnTypes.add(ValueDesc.fromTypeString(column.substring(0, index)));
      }
      return new Simple(columnNames, columnTypes);
    }

    protected final List<String> columnNames;
    protected final List<ValueDesc> columnTypes;

    @JsonCreator
    public Simple(
        @JsonProperty("columnNames") List<String> columnNames,
        @JsonProperty("columnTypes") List<ValueDesc> columnTypes
    )
    {
      this.columnNames = columnNames;
      this.columnTypes = columnTypes;
    }

    @Override
    @JsonProperty
    public List<String> getColumnNames()
    {
      return columnNames;
    }

    @Override
    @JsonProperty
    public List<ValueDesc> getColumnTypes()
    {
      return columnTypes;
    }

    @Override
    public ValueDesc resolve(String column)
    {
      int index = columnNames.indexOf(column);
      if (index >= 0) {
        return columnTypes.get(index);
      }
      ValueDesc resolved = null;
      for (int x = column.lastIndexOf('.'); resolved == null && x > 0; x = column.lastIndexOf('.', x - 1)) {
        resolved = findElementOfStruct(column.substring(0, x), column.substring(x + 1));
      }
      return resolved;
    }

    private ValueDesc findElementOfStruct(String column, String element)
    {
      int index = columnNames.indexOf(column);
      if (index >= 0) {
        ValueDesc type = columnTypes.get(index);
        String[] description = type.getDescription();
        if (type.isStruct() && description != null) {
          for (int i = 1; i < description.length; i++) {
            int split = description[i].indexOf(':');
            if (element.equals(description[i].substring(0, split))) {
              return ValueDesc.of(description[i].substring(split + 1));
            }
          }
        }
      }
      return null;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(columnNames, columnTypes);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RowSignature)) {
        return false;
      }

      RowSignature that = (RowSignature) o;

      if (!Objects.equals(columnTypes, that.getColumnTypes())) {
        return false;
      }
      return Objects.equals(columnNames, that.getColumnNames());
    }

    @Override
    public String toString()
    {
      final StringBuilder s = new StringBuilder("{");
      for (int i = 0; i < columnNames.size(); i++) {
        if (i > 0) {
          s.append(", ");
        }
        final String columnName = columnNames.get(i);
        final ValueDesc columnType = columnTypes.get(i);
        s.append(columnName).append(":").append(columnType);
      }
      return s.append("}").toString();
    }
  }
}
