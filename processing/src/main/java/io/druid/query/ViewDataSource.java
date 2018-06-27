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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.query.filter.DimFilter;
import io.druid.segment.VirtualColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@JsonTypeName("view")
public class ViewDataSource implements DataSource
{
  public static ViewDataSource of(String dataSource, String... columns)
  {
    return new ViewDataSource(dataSource, Arrays.asList(columns), null, null, false);
  }

  @JsonProperty
  private final String name;

  @JsonProperty
  private final List<String> columns;

  @JsonProperty
  private final List<VirtualColumn> virtualColumns;

  @JsonProperty
  private final DimFilter filter;

  @JsonProperty
  private final boolean lowerCasedOutput; // for hive integration

  @JsonCreator
  public ViewDataSource(
      @JsonProperty("name") String name,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("lowerCasedOutput") boolean lowerCasedOutput
  )
  {
    this.name = Preconditions.checkNotNull(name);
    this.columns = columns == null ? ImmutableList.<String>of() : columns;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.filter = filter;
    this.lowerCasedOutput = lowerCasedOutput;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> getNames()
  {
    return Arrays.asList(name);
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  public boolean isLowerCasedOutput()
  {
    return lowerCasedOutput;
  }

  public ViewDataSource withColumns(List<String> columns)
  {
    return new ViewDataSource(name, columns, virtualColumns, filter, lowerCasedOutput);
  }

  public ViewDataSource withFilter(DimFilter filter)
  {
    return new ViewDataSource(name, columns, virtualColumns, filter, lowerCasedOutput);
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof ViewDataSource) || !super.equals(o)) {
      return false;
    }

    ViewDataSource that = (ViewDataSource) o;

    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(columns, that.columns)) {
      return false;
    }
    if (!Objects.equals(virtualColumns, that.virtualColumns)) {
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
    return Objects.hash(name, columns, virtualColumns, filter);
  }

  @Override
  public String toString()
  {
    return "$view:" + name + columns + (virtualColumns == null ? "" : "(" + virtualColumns + ")");
  }
}
