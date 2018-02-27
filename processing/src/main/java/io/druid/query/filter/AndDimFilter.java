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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.math.expr.Expression.AndExpression;
import io.druid.query.Druids;
import io.druid.segment.filter.AndFilter;
import io.druid.segment.filter.Filters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class AndDimFilter implements DimFilter, AndExpression
{
  public static DimFilter of(DimFilter... filters)
  {
    List<DimFilter> list = Lists.newArrayList(Iterables.filter(Arrays.asList(filters), Predicates.notNull()));
    return list.isEmpty() ? null : list.size() == 1 ? list.get(0) : new AndDimFilter(list);
  }

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
  public byte[] getCacheKey()
  {
    return DimFilterCacheHelper.computeCacheKey(DimFilterCacheHelper.AND_CACHE_ID, fields);
  }

  @Override
  public DimFilter optimize()
  {
    List<DimFilter> elements = DimFilters.optimize(fields);
    return elements.size() == 1 ? elements.get(0) : Druids.newAndDimFilterBuilder().fields(elements).build();
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
  public Filter toFilter()
  {
    return new AndFilter(Filters.toFilters(fields));
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

    if (fields != null ? !fields.equals(that.fields) : that.fields != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return fields != null ? fields.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return String.format("(%s)", AND_JOINER.join(fields));
  }
}
