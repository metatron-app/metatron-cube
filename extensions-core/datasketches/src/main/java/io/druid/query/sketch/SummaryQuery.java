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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.Result;
import io.druid.query.UnionAllQuery;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("summary")
public class SummaryQuery extends BaseQuery<Result<Map<String, Object>>>
    implements BaseQuery.RewritingQuery<Result<Map<String, Object>>>,
    Query.MetricSupport<Result<Map<String, Object>>>
{
  private final DimFilter dimFilter;
  private final List<DimensionSpec> dimensions;
  private final List<String> metrics;
  private final List<VirtualColumn> virtualColumns;
  private final boolean includeTimeStats;

  @JsonCreator
  public SummaryQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("includeTimeStats") boolean includeTimeStats,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.dimFilter = filter;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.metrics = metrics == null ? ImmutableList.<String>of() : metrics;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.includeTimeStats = includeTimeStats;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper)
  {
    final Map<String, Map<String, MutableInt>> results = QueryUtils.analyzeTypes(segmentWalker, this);
    Map<String, String> majorTypes = Maps.newHashMap();
    Map<String, Map<String, Integer>> typeDetail = Maps.newHashMap();
    for (Map.Entry<String, Map<String, MutableInt>> entry : results.entrySet()) {
      String column = entry.getKey();
      Map<String, MutableInt> value = entry.getValue();
      if (value.size() == 1) {
        String type = Iterables.getOnlyElement(value.keySet());
        MutableInt count = Iterables.getOnlyElement(value.values());
        majorTypes.put(column, type);
        typeDetail.put(column, ImmutableMap.of(type, count.intValue()));
        continue;
      }
      int max = -1;
      String major = null;
      Map<String, Integer> detail = Maps.newHashMap();
      for (Map.Entry<String, MutableInt> x : value.entrySet()) {
        String type = x.getKey();
        int count = x.getValue().intValue();
        if (max < 0 || max <= count) {
          max = count;
          major = type;
        }
        detail.put(type, count);
      }
      majorTypes.put(column, major);
      typeDetail.put(column, detail);
    }

    Map<String, Object> context = Maps.newHashMap(getContext());
    context.put("majorTypes", majorTypes);

    SketchQuery quantile = new SketchQuery(
        getDataSource(), getQuerySegmentSpec(), dimFilter, virtualColumns, dimensions, metrics, 8192, SketchOp.QUANTILE,
        Maps.newHashMap(context)
    );
    SketchQuery theta = new SketchQuery(
        getDataSource(), getQuerySegmentSpec(), dimFilter, virtualColumns, dimensions, metrics, null, SketchOp.THETA,
        Maps.newHashMap(context)
    );
    Map<String, Object> postProcessor = ImmutableMap.<String, Object>of(
        QueryContextKeys.POST_PROCESSING,
        ImmutableMap.of("type", "sketch.summary", "includeTimeStats", includeTimeStats, "typeDetail", typeDetail)
    );
    Map<String, Object> joinContext = computeOverridenContext(postProcessor);

    return new UnionAllQuery(null, Arrays.asList(quantile, theta), false, -1, -1, -1, joinContext);
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null || super.hasFilters();
  }

  @Override
  public String getType()
  {
    return "summary";
  }

  @Override
  @JsonProperty
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @Override
  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @Override
  @JsonProperty
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  @JsonProperty
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public boolean isIncludeTimeStats()
  {
    return includeTimeStats;
  }

  @Override
  public SummaryQuery withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new SummaryQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        metrics,
        dimFilter,
        includeTimeStats,
        getContext()
    );
  }

  @Override
  public MetricSupport<Result<Map<String, Object>>> withMetrics(List<String> metrics)
  {
    return new SummaryQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        metrics,
        dimFilter,
        includeTimeStats,
        getContext()
    );
  }

  @Override
  public SummaryQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new SummaryQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        metrics,
        dimFilter,
        includeTimeStats,
        getContext()
    );
  }

  @Override
  public SummaryQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SummaryQuery(
        getDataSource(),
        spec,
        virtualColumns,
        dimensions,
        metrics,
        dimFilter,
        includeTimeStats,
        getContext()
    );
  }

  @Override
  public SummaryQuery withDataSource(DataSource dataSource)
  {
    return new SummaryQuery(
        dataSource,
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        metrics,
        dimFilter,
        includeTimeStats,
        getContext()
    );
  }

  @Override
  public SummaryQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SummaryQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        metrics,
        dimFilter,
        includeTimeStats,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public SummaryQuery withDimFilter(DimFilter dimFilter)
  {
    return new SummaryQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        metrics,
        dimFilter,
        includeTimeStats,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder(64)
        .append("MetatronDimensionSketchQuery{")
        .append("dataSource='").append(getDataSource()).append('\'')
        .append(", querySegmentSpec=").append(getQuerySegmentSpec());

    if (dimFilter != null) {
      builder.append(", dimFilter=").append(dimFilter);
    }
    if (virtualColumns != null && !virtualColumns.isEmpty()) {
      builder.append(", virtualColumns=").append(virtualColumns);
    }
    if (!dimensions.isEmpty()) {
      builder.append(", dimensions=").append(dimensions);
    }
    if (!metrics.isEmpty()) {
      builder.append(", metrics=").append(metrics);
    }
    builder.append(", includeTimeStats=").append(includeTimeStats);
    return builder.toString();
  }
}
