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
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.OrFilter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 */
public class OrDimFilter implements DimFilter, OrExpression
{
  // do nothing but creates instance (see DimFilters.or)
  public static OrDimFilter of(DimFilter... filters)
  {
    return new OrDimFilter(Arrays.asList(filters));
  }

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
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.OR_CACHE_ID)
                  .append(fields);
  }

  @Override
  public DimFilter optimize()
  {
    return optimize(this, DimFilter::optimize);
  }

  @Override
  public DimFilter specialize(Segment segment, List<VirtualColumn> virtualColumns)
  {
    return optimize(this, f -> f.specialize(segment, virtualColumns));
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    return optimize(this, f -> f.withRedirection(mapping));
  }

  private static DimFilter optimize(OrDimFilter or, Function<DimFilter, DimFilter> function)
  {
    boolean changed = false;
    List<DimFilter> optimized = Lists.newArrayList();
    for (DimFilter field : or.fields) {
      DimFilter rewritten = function.apply(field);
      changed |= field != rewritten;
      optimized.add(rewritten);
    }
    return changed ? DimFilters.or(optimized) : optimized.size() == 1 ? optimized.get(0) : or;
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
  @SuppressWarnings("unchecked")
  public List<DimFilter> getChildren()
  {
    return fields;
  }

  @Override
  public double cost(FilterContext context)
  {
    return PICK + fields.stream().mapToDouble(f -> f.cost(context)).sum();
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
