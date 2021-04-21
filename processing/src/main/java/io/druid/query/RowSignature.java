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
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = RowSignature.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "simple", value = RowSignature.class),
    @JsonSubTypes.Type(name = "schema", value = Schema.class)
})
public class RowSignature implements io.druid.data.RowSignature
{
  // this is needed to be implemented by all post processors, but let's do it step by step
  public static interface Evolving
  {
    List<String> evolve(List<String> schema);

    RowSignature evolve(Query query, RowSignature schema);
  }

  public static RowSignature of(List<String> columnNames, List<ValueDesc> columnTypes)
  {
    return new RowSignature(columnNames, columnTypes);
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
    return RowSignature.of(columnNames, columnTypes);
  }

  protected final List<String> columnNames;
  protected final List<ValueDesc> columnTypes;

  @JsonCreator
  public RowSignature(
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

  // for streaming sub query.. we don't have index
  public RowSignature replaceDimensionToString()
  {
    List<ValueDesc> replaced = Lists.newArrayList(getColumnTypes());
    for (int i = 0; i < replaced.size(); i++) {
      if (ValueDesc.isDimension(replaced.get(i))) {
        replaced.set(i, ValueDesc.STRING);
      }
    }
    return RowSignature.of(getColumnNames(), replaced);
  }

  public RowSignature retain(List<String> columns)
  {
    return RowSignature.of(columns, GuavaUtils.transform(columns, name -> resolve(name, ValueDesc.UNKNOWN)));
  }

  public RowSignature concat(RowSignature other)
  {
    return RowSignature.of(
        GuavaUtils.concat(getColumnNames(), other.getColumnNames()),
        GuavaUtils.concat(getColumnTypes(), other.getColumnTypes())
    );
  }

  public RowSignature append(String name, ValueDesc type)
  {
    return RowSignature.of(
        GuavaUtils.concat(getColumnNames(), name),
        GuavaUtils.concat(getColumnTypes(), type)
    );
  }

  public RowSignature relay(Query<?> query, boolean finalzed)
  {
    return Queries.relay(this, query, finalzed);
  }
}
