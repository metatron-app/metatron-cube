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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
@JsonTypeName("explode")
public class ExplodeSpec implements LateralViewSpec, RowSignature.Evolving
{
  public static ExplodeSpec of(String column)
  {
    return of(column, null);
  }

  public static ExplodeSpec of(String column, String alias)
  {
    return new ExplodeSpec(column, alias);
  }

  private final String column;
  private final String alias;

  @JsonCreator
  public ExplodeSpec(
      @JsonProperty("column") String column,
      @JsonProperty("alias") String alias
  )
  {
    this.column = Preconditions.checkNotNull(column, "'column' cannot be null");
    this.alias = alias == null ? column : alias;
  }

  @JsonProperty
  public String getColumn()
  {
    return column;
  }

  @JsonProperty
  public String getAlias()
  {
    return alias;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return column.equals(((ExplodeSpec) o).column) &&
           alias.equals(((ExplodeSpec) o).alias);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(column, alias);
  }

  @Override
  public String toString()
  {
    return "ExplodeSpec{column=" + column + ", alias=" + alias + '}';
  }

  @Override
  public Function<Map<String, Object>, Iterable<Map<String, Object>>> prepare()
  {
    return new Function<Map<String, Object>, Iterable<Map<String, Object>>>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Iterable<Map<String, Object>> apply(final Map<String, Object> input)
      {
        Object value = input.remove(column);
        if (value == null) {
          return Arrays.asList();
        }
        if (value.getClass().isArray()) {
          final int length = Array.getLength(value);
          if (length == 0) {
            return Arrays.asList();
          }
          if (length == 1) {
            Object element = Array.get(value, 0);
            input.put(alias, element);
            return Arrays.asList(input);
          }
          final List<Object> list = Lists.newArrayListWithCapacity(length);
          for (int i = 0; i < length; i++) {
            list.add(Array.get(value, i));
          }
          value = list;
        } else if (value instanceof Collection) {
          Collection<Object> collection = (Collection) value;
          if (collection.isEmpty()) {
            return Arrays.asList();
          }
          if (collection.size() == 1) {
            input.put(alias, Iterables.getOnlyElement(collection));
            return Arrays.asList(input);
          }
        }
        if (!(value instanceof Iterable)) {
          throw new IAE("Only explodes array or iterable, not %s", value.getClass());
        }
        return Iterables.transform((Iterable) value, new Function<Object, Map<String, Object>>()
        {
          @Override
          public Map<String, Object> apply(Object element)
          {
            Map<String, Object> copy = Maps.newHashMap(input);
            copy.put(alias, element);
            return copy;
          }
        });
      }
    };
  }

  @Override
  public List<String> evolve(List<String> schema)
  {
    if (!schema.contains(alias)) {
      schema = GuavaUtils.concat(schema, alias);
    }
    return schema;
  }

  @Override
  public RowSignature evolve(RowSignature schema)
  {
    int index = schema.getColumnNames().indexOf(column);
    if (index < 0) {
      return schema;
    }
    List<String> columnNames = Lists.newArrayList(schema.getColumnNames());
    List<ValueDesc> columnTypes = Lists.newArrayList(schema.getColumnTypes());
    ValueDesc type = schema.getColumnTypes().get(index);
    if (type.isDimension()) {
      columnTypes.set(index, type.subElement(ValueDesc.STRING));
    } else if (type.isArray()) {
      columnTypes.set(index, type.subElement(ValueDesc.UNKNOWN));
    }
    columnNames.set(index, alias);
    return RowSignature.of(columnNames, columnTypes);
  }
}
