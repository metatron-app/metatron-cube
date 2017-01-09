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
import com.google.common.base.Preconditions;
import io.druid.query.filter.DimFilter;

import java.util.List;
import java.util.Objects;

// only for join query
@JsonTypeName("join")
public class JoinDataSource extends TableDataSource
{
  @JsonProperty
  private final List<String> columns;

  @JsonProperty
  private final DimFilter filter;

  @JsonCreator
  public JoinDataSource(
      @JsonProperty("name") String name,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("filter") DimFilter filter
  )
  {
    super(Preconditions.checkNotNull(name));
    this.columns = columns;
    this.filter = filter;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  public DataSource withColumns(List<String> columns)
  {
    return new JoinDataSource(name, columns, filter);
  }

  public DataSource withFilter(DimFilter filter)
  {
    return new JoinDataSource(name, columns, filter);
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof JoinDataSource) || !super.equals(o)) {
      return false;
    }

    JoinDataSource that = (JoinDataSource) o;

    if (!Objects.equals(columns, that.columns)) {
      return false;
    }
    if (!Objects.equals(filter, that.filter)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, columns, filter);
  }

  @Override
  public String toString()
  {
    if (columns == null || columns.isEmpty()) {
      return name;
    }
    return name + columns;
  }
}
