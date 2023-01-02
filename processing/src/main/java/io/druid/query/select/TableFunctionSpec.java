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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.query.filter.DimFilter;

import java.util.List;
import java.util.Objects;

public class TableFunctionSpec
{
  private final List<String> parameters;
  private final DimFilter filter;

  @JsonCreator
  public TableFunctionSpec(
      @JsonProperty("parameters") List<String> parameters,
      @JsonProperty("filter") DimFilter filter
  )
  {
    this.parameters = parameters;
    this.filter = filter;
  }

  @JsonProperty
  public List<String> getParameters()
  {
    return parameters;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public DimFilter getFilter()
  {
    return filter;
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
    TableFunctionSpec that = (TableFunctionSpec) o;
    return parameters.equals(that.parameters) && Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(parameters, filter);
  }

  @Override
  public String toString()
  {
    return "TableFunctionSpec{" +
           "parameters=" + parameters +
           (filter == null ? "" : ", filter=" + filter) +
           '}';
  }
}
