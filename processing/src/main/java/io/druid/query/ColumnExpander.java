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

package io.druid.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;

import java.util.List;
import java.util.Map;

/**
 */
public class ColumnExpander
{
  @VisibleForTesting
  static <T> Query<T> expand(Query<T> query, RowResolver resolver)
  {
    return expand(query, Suppliers.ofInstance(resolver));
  }

  public static <T> Query<T> expand(Query<T> query, Supplier<RowResolver> supplier)
  {
    if (query instanceof Query.ColumnsSupport) {
      Query.ColumnsSupport<T> columnsSupport = (Query.ColumnsSupport<T>) query;
      if (GuavaUtils.isNullOrEmpty(columnsSupport.getColumns())) {
        query = columnsSupport.withColumns(ImmutableList.copyOf(supplier.get().getAllColumnNames()));
      }
    }
    if (query instanceof Query.DimensionSupport) {
      Query.DimensionSupport<T> dimensionSupport = (Query.DimensionSupport<T>) query;
      if (GuavaUtils.isNullOrEmpty(dimensionSupport.getDimensions()) && dimensionSupport.allDimensionsForEmpty()) {
        query = dimensionSupport.withDimensionSpecs(
            DefaultDimensionSpec.toSpec(supplier.get().getDimensionNamesExceptTime())
        );
      }
    }
    if (query instanceof Query.MetricSupport) {
      Query.MetricSupport<T> metricSupport = (Query.MetricSupport<T>) query;
      if (GuavaUtils.isNullOrEmpty(metricSupport.getMetrics()) && metricSupport.allMetricsForEmpty()) {
        RowResolver resolver = supplier.get();
        query = metricSupport.withMetrics(Lists.newArrayList(resolver.getMetricNames()));
      }
    }
    if (query instanceof Query.AggregationsSupport) {
      Query.AggregationsSupport<T> aggrSupport = (Query.AggregationsSupport<T>) query;
      if (GuavaUtils.isNullOrEmpty(aggrSupport.getAggregatorSpecs()) && aggrSupport.allMetricsForEmpty()) {
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
    return query;
  }
}
