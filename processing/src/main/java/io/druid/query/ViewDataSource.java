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

@JsonTypeName("view")
public class ViewDataSource extends TableDataSource
{
  @JsonProperty
  private final List<String> columns;

  @JsonProperty
  private final DimFilter filter;

  @JsonProperty
  private final boolean lowerCasedOutput; // for hive integration

  @JsonCreator
  public ViewDataSource(
      @JsonProperty("name") String name,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("lowerCasedOutput") boolean lowerCasedOutput
  )
  {
    super(Preconditions.checkNotNull(name));
    this.columns = columns;
    this.filter = filter;
    this.lowerCasedOutput = lowerCasedOutput;
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

  @JsonProperty
  public boolean isLowerCasedOutput()
  {
    return lowerCasedOutput;
  }

  public ViewDataSource withColumns(List<String> columns)
  {
    return new ViewDataSource(name, columns, filter, lowerCasedOutput);
  }

  public ViewDataSource withFilter(DimFilter filter)
  {
    return new ViewDataSource(name, columns, filter, lowerCasedOutput);
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof ViewDataSource) || !super.equals(o)) {
      return false;
    }

    ViewDataSource that = (ViewDataSource) o;

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
