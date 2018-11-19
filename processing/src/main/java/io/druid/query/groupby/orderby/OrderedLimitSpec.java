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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.common.Cacheable;
import io.druid.query.QueryCacheHelper;

import java.nio.ByteBuffer;
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
    this.limit = limit == null ? Integer.MAX_VALUE : limit;
    Preconditions.checkArgument(this.limit > 0, "limit[%s] must be >0", limit);
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

  public boolean hasLimit()
  {
    return limit > 0 && limit < Integer.MAX_VALUE;
  }

  @Override
  public byte[] getCacheKey()
  {
    if (columns.isEmpty() && limit == Integer.MAX_VALUE) {
      return new byte[0];
    }
    byte[] columnBytes = QueryCacheHelper.computeCacheKeys(columns);
    ByteBuffer buffer = ByteBuffer.allocate(columnBytes.length + Integer.BYTES);
    return buffer.put(columnBytes)
                 .putInt(limit)
                 .array();
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
