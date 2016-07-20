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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

@JsonTypeName("table")
public class TableDataSource implements DataSource
{
  public static List<TableDataSource> of(List<String> dataSources)
  {
    return Lists.transform(
        dataSources, new Function<String, TableDataSource>()
        {
          @Override
          public TableDataSource apply(String input)
          {
            return new TableDataSource(input);
          }
        }
    );
  }

  @JsonProperty
  private final String name;

  @JsonCreator
  public TableDataSource(@JsonProperty("name") String name)
  {
    this.name = (name == null ? null : name);
  }

  @JsonProperty
  public String getName(){
    return name;
  }

  @Override
  public List<String> getNames()
  {
    return Arrays.asList(name);
  }

  public String toString() { return name; }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableDataSource)) {
      return false;
    }

    TableDataSource that = (TableDataSource) o;

    if (!name.equals(that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return name.hashCode();
  }
}
