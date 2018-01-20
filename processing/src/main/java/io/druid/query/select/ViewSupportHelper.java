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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.Query;
import io.druid.query.RowResolver;
import io.druid.query.TableDataSource;
import io.druid.query.ViewDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.segment.ExprVirtualColumn;
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

  public static <T> Query.DimFilterSupport<T> rewrite(Query.DimFilterSupport<T> query, StorageAdapter adapter)
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
    if (query instanceof Query.AggregationsSupport) {
      Query.AggregationsSupport<T> aggrSupport = (Query.AggregationsSupport<T>)query;
      if (aggrSupport.getAggregatorSpecs().isEmpty() && aggrSupport.allMetricsForEmpty()) {
        query = aggrSupport.withAggregatorSpecs(Lists.newArrayList(adapter.getMetadata().getAggregators()));
      }
    }
    if (query instanceof Query.MetricSupport) {
      Query.MetricSupport<T> metricSupport = (Query.MetricSupport<T>)query;
      if (metricSupport.getMetrics().isEmpty() && metricSupport.allMetricsForEmpty()) {
        query = metricSupport.withMetrics(Lists.newArrayList(adapter.getAvailableMetrics()));
      }
    }
    if (query.getDimFilter() != null) {
      Map<String, String> aliasMapping = aliasMapping(query.getVirtualColumns());
      if (!aliasMapping.isEmpty()) {
        DimFilter optimized = query.getDimFilter().withRedirection(aliasMapping);
        if (query.getDimFilter() != optimized) {
          query = query.withDimFilter(optimized);
        }
      }
    }
    return query;
  }

  // todo we need to retain storage adapter itself with view.columns (or throw exception?)
  @SuppressWarnings("unchecked")
  private static <T> Query.DimFilterSupport<T> rewriteWithView(
      Query.DimFilterSupport<T> baseQuery,
      StorageAdapter adapter
  )
  {
    ViewDataSource view = (ViewDataSource) baseQuery.getDataSource();
    Query.DimFilterSupport<T> query = (Query.DimFilterSupport<T>) rewriteWithView(baseQuery);

    VirtualColumns vcs = VirtualColumns.valueOf(query.getVirtualColumns());
    List<String> dimensions = Lists.newArrayList(adapter.getAvailableDimensions());
    List<String> metrics = Lists.newArrayList(adapter.getAvailableMetrics());
    List<String> retainer = null;
    if (!view.getColumns().isEmpty()) {
      retainer = Lists.newArrayList(view.getColumns());
      dimensions.retainAll(retainer);
      metrics.retainAll(retainer);
    }

    if (query instanceof Query.DimensionSupport) {
      Query.DimensionSupport<T> dimSupport = (Query.DimensionSupport<T>) query;
      List<DimensionSpec> dimensionSpecs = dimSupport.getDimensions();
      if (dimensionSpecs.isEmpty() && dimSupport.allDimensionsForEmpty()) {
        dimSupport = dimSupport.withDimensionSpecs(
            DefaultDimensionSpec.toSpec(dimensions, view.isLowerCasedOutput())
        );
      }
      if (retainer != null) {
        retainer.removeAll(DimensionSpecs.toInputNames(dimSupport.getDimensions()));
      }
      query = dimSupport;
    }
    if (query instanceof Query.AggregationsSupport) {
      Query.AggregationsSupport<T> aggrSupport = (Query.AggregationsSupport<T>) query;
      List<AggregatorFactory> aggregatorSpecs = Lists.newArrayList(aggrSupport.getAggregatorSpecs());
      if (aggregatorSpecs.isEmpty() && aggrSupport.allMetricsForEmpty()) {
        Map<String, AggregatorFactory> aggregators = AggregatorFactory.asMap(adapter.getMetadata().getAggregators());
        for (String metric : metrics) {
          AggregatorFactory aggregator = aggregators.get(metric);
          if (aggregator != null) {
            aggregatorSpecs.add(aggregator);
          }
        }
        query = aggrSupport.withAggregatorSpecs(aggregatorSpecs);
      }
    }
    if (query instanceof Query.MetricSupport) {
      Query.MetricSupport<T> metricSupport = (Query.MetricSupport<T>) query;
      List<String> metricSpecs = Lists.newArrayList(metricSupport.getMetrics());
      if (metricSpecs.isEmpty() && metricSupport.allMetricsForEmpty()) {
        metricSpecs = metrics;
      }
      // add remaining retainers to metrics if vc exists (special handling only for metric support)
      if (retainer != null) {
        retainer.removeAll(metricSpecs);
        for (String remain : retainer) {
          if (vcs.getVirtualColumn(remain) != null) {
            metricSpecs.add(remain);
          }
        }
      }
      query = metricSupport.withMetrics(metricSpecs);
    }
    if (baseQuery != query) {
      log.info("view translated query to %s", query);
    }
    return query;
  }

  private static Query rewriteWithView(Query baseQuery)
  {
    ViewDataSource view = (ViewDataSource) baseQuery.getDataSource();
    if (baseQuery instanceof Query.DimFilterSupport) {
      Query.DimFilterSupport<?>  query = (Query.DimFilterSupport<?>) baseQuery;
      // merge vcs.. in will be used selectively by cursor
      Map<String, VirtualColumn> vcs = VirtualColumns.asMap(view.getVirtualColumns());
      for (VirtualColumn vc : query.getVirtualColumns()) {
        vcs.put(vc.getOutputName(), vc);  // override
      }
      if (!view.getVirtualColumns().isEmpty()) {
        query = query.withVirtualColumns(Lists.newArrayList(vcs.values()));
      }
      DimFilter dimFilter = DimFilters.andNullable(view.getFilter(), query.getDimFilter());
      if (dimFilter != null) {
        Map<String, String> aliasMapping = aliasMapping(query.getVirtualColumns());
        if (!aliasMapping.isEmpty()) {
          dimFilter = dimFilter.withRedirection(aliasMapping);
        }
      }
      if (dimFilter != query.getDimFilter()) {
        query = query.withDimFilter(dimFilter);
      }
      baseQuery = query;
    }
    return baseQuery.withDataSource(TableDataSource.of(view.getName()));
  }

  // some queries uses expression vc as alias.. which disables effective filtering
  private static Map<String, String> aliasMapping(List<VirtualColumn> virtualColumns)
  {
    if (virtualColumns == null || virtualColumns.isEmpty()) {
      return ImmutableMap.of();
    }
    Map<String, String> mapping = Maps.newHashMap();
    for (VirtualColumn vc : virtualColumns) {
      if (vc instanceof ExprVirtualColumn) {
        Expr expr = Parser.parse(((ExprVirtualColumn) vc).getExpression());
        if (Evals.isIdentifier(expr)) {
          mapping.put(vc.getOutputName(), Evals.getIdentifier(expr));
        }
      }
    }
    return mapping;
  }

  public static Schema toSchema(Query.MetricSupport<?> query, StorageAdapter adapter)
  {
    final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
    final List<String> metrics = Lists.newArrayList(query.getMetrics());

    final VirtualColumns virtualColumns = VirtualColumns.valueOf(query.getVirtualColumns(), adapter);
    final RowResolver resolver = new RowResolver(adapter, virtualColumns);

    final List<ValueDesc> columnTypes = Lists.newArrayList();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      if (dimensionSpec.getExtractionFn() == null) {
        columnTypes.add(resolver.resolveColumn(dimensionSpec.getDimension(), ValueDesc.UNKNOWN));
      } else {
        columnTypes.add(ValueDesc.STRING);
      }
    }
    for (String metric : metrics) {
      columnTypes.add(resolver.resolveColumn(metric, ValueDesc.UNKNOWN));
    }
    List<AggregatorFactory> aggregators = Lists.newArrayList();
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
