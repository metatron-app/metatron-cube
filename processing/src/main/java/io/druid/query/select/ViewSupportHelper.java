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
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import io.druid.data.ValueDesc;
import io.druid.query.Query;
import io.druid.query.RowResolver;
import io.druid.query.TableDataSource;
import io.druid.query.ViewDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;

import java.util.List;
import java.util.Map;

/**
 */
public class ViewSupportHelper
{
  private static final Logger log = new Logger(ViewSupportHelper.class);

  public static <T> Query<T> rewrite(Query.DimFilterSupport<T> query, StorageAdapter adapter)
  {
    if (query.getDataSource() instanceof ViewDataSource) {
      return rewriteWithView(query, adapter);
    }
    if (query instanceof Query.DimensionSupport) {
      Query.DimensionSupport<T> dimensionSupport = (Query.DimensionSupport<T>)query;
      if (dimensionSupport.getDimensions().isEmpty() && dimensionSupport.allDimensionsForEmpty()) {
        query = dimensionSupport.withDimensionSpecs(
            DefaultDimensionSpec.toSpec(adapter.getAvailableDimensions())
        );
      }
    }
    if (query instanceof Query.ViewSupport) {
      Query.ViewSupport<T> viewSupport = (Query.ViewSupport<T>)query;
      if (viewSupport.getMetrics().isEmpty() && viewSupport.allMetricsForEmpty()) {
        query = viewSupport.withMetrics(Lists.newArrayList(adapter.getAvailableMetrics()));
      }
    }
    return query;
  }

  public static <T> Query<T> rewriteWithView(Query.DimFilterSupport<T> query, StorageAdapter adapter)
  {
    List<String> dimensions = Lists.newArrayList(adapter.getAvailableDimensions());
    List<String> metrics = Lists.newArrayList(adapter.getAvailableMetrics());

    ViewDataSource view = (ViewDataSource) query.getDataSource();
    List<String> retainers = Lists.newArrayList(view.getColumns());
    if (!retainers.isEmpty()) {
      dimensions.retainAll(retainers);
      metrics.retainAll(retainers);
      retainers.removeAll(dimensions);
      retainers.removeAll(metrics);
    }

    if (query instanceof Query.ViewSupport) {
      Query.ViewSupport<T> viewSupport = (Query.ViewSupport<T>) query;
      List<VirtualColumn> virtualColumns = Lists.newArrayList(viewSupport.getVirtualColumns());
      RowResolver resolver1 = new RowResolver(adapter, VirtualColumns.valueOf(virtualColumns));
      RowResolver resolver2 = new RowResolver(adapter, VirtualColumns.valueOf(view.getVirtualColumns()));
      for (String resolving : Iterables.concat(DimensionSpecs.toInputNames(viewSupport.getDimensions()), retainers)) {
        if (dimensions.contains(resolving) || metrics.contains(resolving)) {
          continue;
        }
        VirtualColumn vc1 = resolver1.resolveVC(resolving);
        VirtualColumn vc2 = resolver2.resolveVC(resolving);
        if (vc1 == null && vc2 != null) {
          virtualColumns.add(vc2);
          if (viewSupport.neededForDimension(resolving)) {
            dimensions.add(resolving);
          } else {
            metrics.add(resolving);
          }
        }
      }
      if (viewSupport.getDimensions().isEmpty()) {
        boolean lowerCasedOutput = view.isLowerCasedOutput();
        viewSupport = viewSupport.withDimensionSpecs(DefaultDimensionSpec.toSpec(dimensions, lowerCasedOutput));
      }
      if (viewSupport.getMetrics().isEmpty()) {
        viewSupport = viewSupport.withMetrics(metrics);
      }
      if (!virtualColumns.isEmpty()) {
        viewSupport = viewSupport.withVirtualColumns(virtualColumns);
      }
      query = viewSupport;
    } else if (query instanceof Query.DimensionSupport) {
      Query.DimensionSupport<T> dimSupport = (Query.DimensionSupport<T>) query;
      List<VirtualColumn> virtualColumns = Lists.newArrayList(dimSupport.getVirtualColumns());
      RowResolver resolver1 = new RowResolver(adapter, VirtualColumns.valueOf(virtualColumns));
      RowResolver resolver2 = new RowResolver(adapter, VirtualColumns.valueOf(view.getVirtualColumns()));
      for (String resolving : Iterables.concat(DimensionSpecs.toInputNames(dimSupport.getDimensions()), retainers)) {
        if (dimensions.contains(resolving) || !dimSupport.neededForDimension(resolving)) {
          continue;
        }
        VirtualColumn vc1 = resolver1.resolveVC(resolving);
        VirtualColumn vc2 = resolver2.resolveVC(resolving);
        if (vc1 == null && vc2 != null) {
          virtualColumns.add(vc2);
          dimensions.add(resolving);
        }
      }
      if (dimSupport.getDimensions().isEmpty()) {
        boolean lowerCasedOutput = view.isLowerCasedOutput();
        dimSupport = dimSupport.withDimensionSpecs(DefaultDimensionSpec.toSpec(dimensions, lowerCasedOutput));
      }
      if (!virtualColumns.isEmpty()) {
        dimSupport = dimSupport.withVirtualColumns(virtualColumns);
      }
      query = dimSupport;
    }

    DimFilter dimFilter = view.getFilter();
    if (dimFilter != null) {
      if (query.getDimFilter() != null) {
        dimFilter = AndDimFilter.of(dimFilter, query.getDimFilter());
      }
      query = query.withDimFilter(dimFilter);
    }
    query = (Query.DimFilterSupport<T>) query.withDataSource(new TableDataSource(view.getName()));
    log.info("view translated query to %s", query);
    return query;
  }

  public static Schema toSchema(Query.ViewSupport<?> query, Segment segment)
  {
    final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
    final List<String> metrics = Lists.newArrayList(query.getMetrics());
    final VirtualColumns virtualColumns = VirtualColumns.valueOf(query.getVirtualColumns());

    final RowResolver resolver = new RowResolver(segment.asStorageAdapter(false), virtualColumns);

    final List<ValueDesc> columnTypes = Lists.newArrayList();
    for (String columnName : Iterables.concat(dimensions, metrics)) {
      columnTypes.add(resolver.resolveColumn(columnName, ValueDesc.UNKNOWN));
    }
    List<AggregatorFactory> aggregators = Lists.newArrayList();
    StorageAdapter adapter = segment.asStorageAdapter(false);
    if (adapter.getMetadata() != null && adapter.getMetadata().getAggregators() != null) {
      Map<String, AggregatorFactory> factoryMap = Maps.newHashMap();
      for (AggregatorFactory aggregator : adapter.getMetadata().getAggregators()) {
        factoryMap.put(aggregator.getName(), aggregator);
      }
      for (String metric : metrics) {
        aggregators.add(factoryMap.get(metric));
      }
    }

    return new Schema(dimensions, metrics, columnTypes, aggregators);
  }
}
