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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.query.Query;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.GroupByQuery;
import io.druid.segment.filter.Filters;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

// todo schema evolving
public class TableFunctionSpec implements Cacheable
{
  public static TableFunctionSpec from(GroupByQuery query)
  {
    Set<String> grouped = getGroupedDimensions(query);
    if (!grouped.isEmpty() && DimensionSpecs.isAllDefault(query.getDimensions())) {
      List<String> dimensions = query.getDimensions().stream().map(d -> d.getDimension())
                                     .filter(c -> grouped.contains(c)).collect(Collectors.toList());
      if (!dimensions.isEmpty()) {
        DimFilter rewritten = DimFilters.rewrite(
            query.getFilter(), f -> GuavaUtils.containsAny(Filters.getDependents(f), dimensions) ? f : null
        );
        if (dimensions.size() > 1 || rewritten != null) {
          return new TableFunctionSpec("explode", dimensions, rewritten);
        }
      }
    }
    return null;
  }

  private static Set<String> getGroupedDimensions(GroupByQuery query)
  {
    String value = query.getContextValue(Query.GROUPED_DIMENSIONS, null);
    return StringUtils.isNullOrEmpty(value) ? ImmutableSet.of() : Sets.newHashSet(StringUtils.splitAndTrim(value, ','));
  }

  private final String operation;
  private final List<String> parameters;
  private final DimFilter filter;

  @JsonCreator
  public TableFunctionSpec(
      @JsonProperty("operation") String operation,
      @JsonProperty("parameters") List<String> parameters,
      @JsonProperty("filter") DimFilter filter
  )
  {
    this.operation = operation;
    this.parameters = parameters;
    this.filter = filter;
  }

  @JsonProperty
  public String getOperation()
  {
    return operation;
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
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(operation).append(parameters).append(filter);
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
    return operation.equals(that.operation) &&
           parameters.equals(that.parameters) &&
           Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(operation, parameters, filter);
  }

  @Override
  public String toString()
  {
    return "TableFunctionSpec{" +
           "operation=" + operation +
           "parameters=" + parameters +
           (filter == null ? "" : ", filter=" + filter) +
           '}';
  }
}
