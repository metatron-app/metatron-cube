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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.Query;
import io.druid.query.RowResolver;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;

/**
 */
public class ViewSupportHelper
{
  public static <T> Query<T> rewrite(Query<T> query, Supplier<RowResolver> resolver)
  {
    return query instanceof Query.VCSupport ? rewrite((Query.VCSupport<T>) query, resolver) : query;
  }

  public static <T> Query.VCSupport<T> rewrite(Query.VCSupport<T> query, RowResolver resolver)
  {
    return rewrite(query, Suppliers.ofInstance(resolver));
  }

  private static <T> Query.VCSupport<T> rewrite(Query.VCSupport<T> query, Supplier<RowResolver> supplier)
  {
    if (query instanceof Query.DimensionSupport) {
      Query.DimensionSupport<T> dimensionSupport = (Query.DimensionSupport<T>)query;
      if (dimensionSupport.getDimensions().isEmpty() && dimensionSupport.allDimensionsForEmpty()) {
        query = dimensionSupport.withDimensionSpecs(
            DefaultDimensionSpec.toSpec(supplier.get().getDimensionNames())
        );
      }
    }
    if (query instanceof Query.AggregationsSupport) {
      Query.AggregationsSupport<T> aggrSupport = (Query.AggregationsSupport<T>)query;
      if (aggrSupport.getAggregatorSpecs().isEmpty() && aggrSupport.allMetricsForEmpty()) {
        RowResolver resolver = supplier.get();
        List<AggregatorFactory> aggregators = Lists.newArrayList();
        Map<String, AggregatorFactory> aggregatorsMap = resolver.getAggregators();
        for (String metric : resolver.getMetricNames()) {
          AggregatorFactory aggregator = aggregatorsMap.get(metric);
          if (aggregator != null) {
            aggregators.add(aggregator);
          }
        }
        query = aggrSupport.withAggregatorSpecs(aggregators);
      }
    }
    if (query instanceof Query.MetricSupport) {
      Query.MetricSupport<T> metricSupport = (Query.MetricSupport<T>)query;
      if (metricSupport.getMetrics().isEmpty() && metricSupport.allMetricsForEmpty()) {
        RowResolver resolver = supplier.get();
        query = metricSupport.withMetrics(Lists.newArrayList(resolver.getMetricNames()));
      }
    }
    if (query instanceof Query.DimFilterSupport) {
      Query.DimFilterSupport<T> filterSupport = (Query.DimFilterSupport<T>) query;
      if (filterSupport.getDimFilter() != null) {
        Map<String, String> aliasMapping = aliasMapping(query.getVirtualColumns());
        if (!aliasMapping.isEmpty()) {
          DimFilter optimized = filterSupport.getDimFilter().withRedirection(aliasMapping);
          if (filterSupport.getDimFilter() != optimized) {
            query = filterSupport.withDimFilter(optimized);
          }
        }
      }
    }
    return query;
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
        if (Evals.isIdentifier(expr) && !Evals.getIdentifier(expr).equals(vc.getOutputName())) {
          mapping.put(vc.getOutputName(), Evals.getIdentifier(expr));
        }
      }
    }
    return mapping;
  }
}
