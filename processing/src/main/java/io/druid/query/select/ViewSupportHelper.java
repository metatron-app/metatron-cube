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

package io.druid.query.select;

import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.ViewDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.segment.StorageAdapter;

import java.util.Set;

/**
 */
public class ViewSupportHelper
{
  public static <T> Query<T> rewrite(Query.DimFilterSupport<T> query, StorageAdapter adapter)
  {
    if (query.getDataSource() instanceof ViewDataSource) {
      ViewDataSource view = (ViewDataSource)query.getDataSource();
      return rewrite(query, Sets.newHashSet(view.getColumns()), view.isLowerCasedOutput(), view.getFilter(), adapter);
    }
    return rewrite(query, null, false, null, adapter);
  }

  public static boolean hasFilter(DataSource dataSource)
  {
    return dataSource instanceof ViewDataSource && ((ViewDataSource) dataSource).getFilter() != null;
  }

  private static <T> Query.DimFilterSupport<T> rewrite(
      Query.DimFilterSupport<T> query,
      Set<String> retainers,
      boolean lowerCasedOutput,
      DimFilter dimFilter,
      StorageAdapter adapter
  )
  {
    if (query instanceof Query.DimensionSupport) {
      Query.DimensionSupport<T> dimensionSupport = (Query.DimensionSupport<T>)query;
      if (dimensionSupport.getDimensions() == null || dimensionSupport.getDimensions().isEmpty()) {
        if (retainers != null || dimensionSupport.allDimensionsForEmpty()) {
          dimensionSupport = dimensionSupport.withDimensionSpecs(
              DefaultDimensionSpec.toSpec(
                  GuavaUtils.retain(adapter.getAvailableDimensions(), retainers), lowerCasedOutput
              )
          );
        }
      }
      query = dimensionSupport;
    }
    if (query instanceof Query.ViewSupport) {
      Query.ViewSupport<T> viewSupport = (Query.ViewSupport<T>)query;
      if (viewSupport.getMetrics() == null || viewSupport.getMetrics().isEmpty()) {
        if (retainers != null || viewSupport.allMetricsForEmpty()) {
          viewSupport = viewSupport.withMetrics(GuavaUtils.retain(adapter.getAvailableMetrics(), retainers));
        }
      }
      query = viewSupport;
    }
    if (dimFilter != null) {
      if (query.getDimFilter() != null) {
        dimFilter = AndDimFilter.of(dimFilter, query.getDimFilter());
      }
      query = query.withDimFilter(dimFilter);
    }
    return query;
  }
}
