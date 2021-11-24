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
  public static LimitSpec of(Integer limit)
  {
    return limit == null ? NoopLimitSpec.INSTANCE : new LimitSpec(null, limit, null, null, null, null);
  }

  public static LimitSpec of(Integer limit, OrderByColumnSpec... specs)
  {
    return limit == null && specs.length == 0 ?
           NoopLimitSpec.INSTANCE : new LimitSpec(Arrays.asList(specs), limit, null, null, null, null);
  }

  public static boolean inGroupByOrdering(BaseAggregationQuery query, OrderedLimitSpec ordering)
  {
    return GuavaUtils.isNullOrEmpty(ordering.getColumns()) ||
           inGroupByOrdering(ordering.getColumns(), query.getDimensions());
  }

  public static boolean inGroupByOrdering(List<OrderByColumnSpec> orderByColumns, List<DimensionSpec> dimensions)
  {
    if (orderByColumns.size() > dimensions.size()) {
      return false;
    }
    for (int i = 0; i < orderByColumns.size(); i++) {
      OrderByColumnSpec orderBy = orderByColumns.get(i);
      if (!orderBy.equals(DimensionSpecs.asOrderByColumnSpec(dimensions.get(i)))) {
        return false;
      }
    }
    return true;
  }
}
