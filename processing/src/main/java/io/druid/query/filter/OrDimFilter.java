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
import io.druid.math.expr.Expression.OrExpression;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.OrFilter;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class OrDimFilter implements DimFilter, OrExpression
{
  private static final Joiner OR_JOINER = Joiner.on(" || ");

  final private List<DimFilter> fields;

  @JsonCreator
  public OrDimFilter(
      @JsonProperty("fields") List<DimFilter> fields
  )
  {
    fields = DimFilters.filterNulls(fields);
    Preconditions.checkArgument(fields.size() > 0, "OR operator requires at least one field");
    this.fields = fields;
  }

  @JsonProperty
  public List<DimFilter> getFields()
  {
    return fields;
  }

  @Override
  public byte[] getCacheKey()
  {
    return KeyBuilder.get()
                     .append(DimFilterCacheHelper.OR_CACHE_ID)
                     .append(fields)
                     .build();
  }

  @Override
  public DimFilter optimize()
  {
    return DimFilters.or(DimFilters.optimize(fields));
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
    return changed ? new OrDimFilter(filters) : this;
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
    return new OrFilter(Filters.toFilters(fields, resolver));
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

    OrDimFilter that = (OrDimFilter) o;

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
    return String.format("(%s)", OR_JOINER.join(fields));
  }
}
