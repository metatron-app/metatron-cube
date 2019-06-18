/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.metamx.common.guava.nary.BinaryFn;

import java.util.Map;

/**
 */
public class TableDesc
{
  private final String comment;
  private final Map<String, String> properties;
  private final Map<String, ColumnDesc> columnDescs;

  @JsonCreator
  public TableDesc(
      @JsonProperty("comment") String comment,
      @JsonProperty("properties") Map<String, String> properties,
      @JsonProperty("columnDescs") Map<String, ColumnDesc> columnDescs
  )
  {
    this.comment = comment;
    this.properties = properties;
    this.columnDescs = columnDescs;
  }

  @JsonProperty
  public String getComment()
  {
    return comment;
  }

  @JsonProperty
  public Map<String, String> getProperties()
  {
    return properties;
  }

  @JsonProperty
  public Map<String, ColumnDesc> getColumnDescs()
  {
    return columnDescs;
  }

  @Override
  public String toString()
  {
    return "TableDesc{" +
           "comment='" + comment + '\'' +
           ", properties=" + properties +
           ", columnDescs=" + columnDescs +
           '}';
  }

  public TableDesc update(TableDesc other)
  {
    Map<String, String> newProperties = merge(properties, other.properties, null);
    Map<String, ColumnDesc> newColumns = merge(columnDescs, other.columnDescs, ColumnDesc.MERGER);
    return new TableDesc(other.comment == null ? comment : other.comment, newProperties, newColumns);
  }

  static <T> Map<String, T> merge(Map<String, T> oldbie, Map<String, T> newbie, BinaryFn<T, T, T> merger) {
    if (oldbie == null) {
      return newbie;
    }
    if (newbie == null) {
      return oldbie;
    }
    Map<String, T> newColumns = Maps.newLinkedHashMap(oldbie);
    for (Map.Entry<String, T> entry : newbie.entrySet()) {
      if (entry.getValue() == null) {
        newColumns.remove(entry.getKey());
      } else if (merger == null) {
        newColumns.put(entry.getKey(), entry.getValue());
      } else {
        T prev = newColumns.get(entry.getKey());
        if (prev == null) {
          newColumns.put(entry.getKey(), entry.getValue());
        } else {
          newColumns.put(entry.getKey(), merger.apply(prev, entry.getValue()));
        }
      }
    }
    return newColumns;
  }
}
