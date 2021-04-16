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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;

import java.util.Arrays;
import java.util.List;

/**
 */
public class LimitSpecs
{
  public static final Function<OrderByColumnSpec, String> GET_DIMENSION = new Function<OrderByColumnSpec, String>()
  {
    @Override
    public String apply(OrderByColumnSpec input)
    {
      return input.getDimension();
    }
  };

  public static LimitSpec of(Integer limit)
  {
    return limit == null ? NoopLimitSpec.INSTANCE : new LimitSpec(null, limit, null, null, null, null);
  }

  public static LimitSpec of(Integer limit, OrderByColumnSpec... specs)
  {
    return limit == null && specs.length == 0 ?
           NoopLimitSpec.INSTANCE : new LimitSpec(Arrays.asList(specs), limit, null, null, null, null);
  }

  public static boolean isDummy(LimitSpec limitSpec)
  {
    return GuavaUtils.isNullOrEmpty(limitSpec.getColumns()) &&
           GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs()) &&
           !limitSpec.hasLimit();
  }

  public static List<String> getColumns(List<? extends OrderByColumnSpec> orderByColumnSpecs)
  {
    return GuavaUtils.isNullOrEmpty(orderByColumnSpecs) ?
           ImmutableList.<String>of() :
           ImmutableList.copyOf(Lists.transform(orderByColumnSpecs, GET_DIMENSION));
  }

  public static String[] getColumnsAsArray(List<? extends OrderByColumnSpec> orderByColumnSpecs)
  {
    return getColumns(orderByColumnSpecs).toArray(new String[orderByColumnSpecs.size()]);
  }

  public static boolean inGroupByOrdering(BaseAggregationQuery query, OrderedLimitSpec ordering)
  {
    return inGroupByOrdering(query.getLimitOrdering(ordering), query.getDimensions());
  }

  public static boolean inGroupByOrdering(List<OrderByColumnSpec> orderByColumns, List<DimensionSpec> dimensions)
  {
    if (orderByColumns.size() > dimensions.size()) {
      return false;
    }
    List<OrderByColumnSpec> dimensionOrdering = DimensionSpecs.asOrderByColumnSpec(dimensions);
    for (int i = 0; i < orderByColumns.size(); i++) {
      OrderByColumnSpec orderBy = orderByColumns.get(i);
      if (!orderBy.equals(dimensionOrdering.get(i))) {
        return false;
      }
    }
    return true;
  }
}
