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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.ViewDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;

import java.util.Collection;
import java.util.List;

/**
 */
public class ViewSupportHelper
{
  private static final Logger log = new Logger(ViewSupportHelper.class);

  public static <T> Query<T> rewrite(Query.DimFilterSupport<T> query, StorageAdapter adapter)
  {
    DataSource dataSource = query.getDataSource();

    Collection<String> retainers = null;
    Collection<String> exclusions = null;
    List<VirtualColumn> virtualColumns = Lists.newArrayList();
    DimFilter dimFilter = null;
    boolean lowerCasedOutput = false;     // to lessen pain of hive integration

    boolean viewDataSourced = dataSource instanceof ViewDataSource;
    if (viewDataSourced) {
      ViewDataSource view = (ViewDataSource) dataSource;
      retainers = Sets.newHashSet(view.getColumns());
      exclusions = Sets.newHashSet(view.getColumnExclusions());
      virtualColumns = GuavaUtils.<VirtualColumn>concat(virtualColumns, view.getVirtualColumns());
      dimFilter = view.getFilter();
      lowerCasedOutput = view.isLowerCasedOutput();
    }

    VirtualColumns virtualColumn = VirtualColumns.valueOf(virtualColumns);
    if (query instanceof Query.DimensionSupport) {
      Query.DimensionSupport<T> dimensionSupport = (Query.DimensionSupport<T>)query;
      if (dimensionSupport.getDimensions() == null || dimensionSupport.getDimensions().isEmpty()) {
        if (viewDataSourced || dimensionSupport.allDimensionsForEmpty()) {
          List<String> availableDimensions =
              GuavaUtils.exclude(GuavaUtils.retain(adapter.getAvailableDimensions(), retainers), exclusions);
          for (String remain : GuavaUtils.exclude(retainers, availableDimensions)) {
            if (virtualColumn.getVirtualColumn(remain) != null) {
              availableDimensions.add(remain);
            }
          }
          if (!availableDimensions.isEmpty()) {
            dimensionSupport = dimensionSupport.withDimensionSpecs(
                DefaultDimensionSpec.toSpec(availableDimensions, lowerCasedOutput)
            );
          }
        }
      }
      if (!virtualColumns.isEmpty() && virtualColumns != dimensionSupport.getVirtualColumns()) {
        dimensionSupport = dimensionSupport.withVirtualColumns(virtualColumns);
      }
      query = dimensionSupport;
    }
    if (query instanceof Query.ViewSupport) {
      Query.ViewSupport<T> viewSupport = (Query.ViewSupport<T>)query;
      if (viewSupport.getMetrics() == null || viewSupport.getMetrics().isEmpty()) {
        if (viewDataSourced || viewSupport.allMetricsForEmpty()) {
          List<String> availableMetrics =
              GuavaUtils.exclude(GuavaUtils.retain(adapter.getAvailableMetrics(), retainers), exclusions);
          for (String remain : GuavaUtils.exclude(retainers, availableMetrics)) {
            if (virtualColumn.getVirtualColumn(remain) != null) {
              availableMetrics.add(remain);
            }
          }
          if (!availableMetrics.isEmpty()) {
            viewSupport = viewSupport.withMetrics(availableMetrics);
          }
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
    if (viewDataSourced) {
      query.withDataSource(new TableDataSource(Iterables.getOnlyElement(dataSource.getNames())));
      log.info("view translated query to %s", query);
    }
    return query;
  }
}
