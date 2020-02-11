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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.math.expr.Expression.AndExpression;
import io.druid.segment.Segment;
import io.druid.segment.filter.AndFilter;
import io.druid.segment.filter.Filters;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class AndDimFilter implements DimFilter, AndExpression
{
  private static final Joiner AND_JOINER = Joiner.on(" && ");

  final private List<DimFilter> fields;

  @JsonCreator
  public AndDimFilter(
      @JsonProperty("fields") List<DimFilter> fields
  )
  {
    fields = DimFilters.filterNulls(fields);
    Preconditions.checkArgument(fields.size() > 0, "AND operator requires at least one field");
    this.fields = fields;
  }

  @JsonProperty
  public List<DimFilter> getFields()
  {
    return fields;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheHelper.AND_CACHE_ID)
                  .append(fields);
  }

  @Override
  public DimFilter optimize(Segment segment)
  {
    return DimFilters.and(DimFilters.optimize(fields));
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    boolean changed = false;
    List<DimFilter> filters = Lists.newArrayList();
    for (DimFilter filter : fields) {
      DimFilter optimized = filter.withRedirection(mapping);
      changed |= optimized != filter;
      filters.add(optimized);
    }
    return changed ? new AndDimFilter(filters) : this;
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    for (DimFilter filter : fields) {
      filter.addDependent(handler);
    }
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new AndFilter(Filters.toFilters(fields, resolver));
  }

  @Override
  public List<DimFilter> getChildren()
  {
    return fields;
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

    AndDimFilter that = (AndDimFilter) o;

    if (!fields.equals(that.fields)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return fields.hashCode();
  }

  @Override
  public String toString()
  {
    return String.format("(%s)", AND_JOINER.join(fields));
  }
}
