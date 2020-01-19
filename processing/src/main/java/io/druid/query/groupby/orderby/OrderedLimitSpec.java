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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;

import java.util.Arrays;
import java.util.List;

/**
 */
public class OrderedLimitSpec implements Cacheable
{
  public static OrderedLimitSpec of(int limit, OrderByColumnSpec... orderings)
  {
    return new OrderedLimitSpec(Arrays.asList(orderings), limit);
  }

  protected final List<OrderByColumnSpec> columns;
  protected final int limit;

  @JsonCreator
  public OrderedLimitSpec(
      @JsonProperty("columns") List<OrderByColumnSpec> columns,
      @JsonProperty("limit") Integer limit)
  {
    this.columns = columns == null ? ImmutableList.<OrderByColumnSpec>of() : columns;
    this.limit = limit == null ? -1 : limit;
  }

  @JsonProperty
  public List<OrderByColumnSpec> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  public OrderedLimitSpec withOrderingSpec(List<OrderByColumnSpec> columns)
  {
    return new OrderedLimitSpec(columns, limit);
  }

  public OrderedLimitSpec withOrderingIfNotExists(List<OrderByColumnSpec> ordering)
  {
    return !GuavaUtils.isNullOrEmpty(ordering) && GuavaUtils.isNullOrEmpty(columns) ? withOrderingSpec(ordering) : this;
  }

  public boolean hasLimit()
  {
    return limit > 0;
  }

  public boolean isNoop()
  {
    return GuavaUtils.isNullOrEmpty(columns) && !hasLimit();
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(columns)
                  .append(limit);
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

    OrderedLimitSpec that = (OrderedLimitSpec) o;

    if (limit != that.limit) {
      return false;
    }
    if (!columns.equals(that.columns)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = columns.hashCode();
    result = 31 * result + limit;
    return result;
  }

  @Override
  public String toString()
  {
    return "OrderedLimitSpec{" +
           "columns=" + columns +
           ", limit=" + limit +
           '}';
  }
}
