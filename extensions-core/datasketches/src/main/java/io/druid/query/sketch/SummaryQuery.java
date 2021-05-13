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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.data.ValueDesc;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
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
    implements Query.RewritingQuery<Result<Map<String, Object>>>,
    Query.MetricSupport<Result<Map<String, Object>>>
{
  private final DimFilter filter;
  private final List<DimensionSpec> dimensions;
  private final List<String> metrics;
  private final List<VirtualColumn> virtualColumns;
  private final int round;
  private final boolean includeTimeStats;
  private final boolean includeCovariance;

  @JsonCreator
  public SummaryQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("round") int round,
      @JsonProperty("includeTimeStats") boolean includeTimeStats,
      @JsonProperty("includeCovariance") boolean includeCovariance,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.filter = filter;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.metrics = metrics == null ? ImmutableList.<String>of() : metrics;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.round = round;
    this.includeTimeStats = includeTimeStats;
    this.includeCovariance = includeCovariance;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    Map<String, Map<ValueDesc, MutableInt>> results = QueryUtils.analyzeTypes(segmentWalker, this);

    Map<String, String> majorTypes = Maps.newHashMap();
    Map<String, Map<String, Integer>> typeDetail = Maps.newHashMap();

    for (Map.Entry<String, Map<ValueDesc, MutableInt>> entry : results.entrySet()) {
      String column = entry.getKey();
      Map<ValueDesc, MutableInt> value = Maps.newHashMap();
      int majorCount = -1;
      ValueDesc majorType = null;
      for (Map.Entry<ValueDesc, MutableInt> e : entry.getValue().entrySet()) {
        ValueDesc type = e.getKey();
        if (type.isDimension()) {
          type = ValueDesc.STRING;
        }
        value.put(type, e.getValue());
        if (majorType == null || e.getValue().intValue() > majorCount) {
          majorType = type;
          majorCount = e.getValue().intValue();
        }
      }
      if (majorType == null) {
        continue;
      }
      Map<String, Integer> detail = Maps.newHashMap();
      for (Map.Entry<ValueDesc, MutableInt> e : value.entrySet()) {
        detail.put(e.getKey().typeName(), e.getValue().intValue());
      }
      majorTypes.put(column, majorType.typeName());
      typeDetail.put(column, detail);
    }

    Map<String, Object> sketchContext = BaseQuery.copyContext(this);
    if (!majorTypes.isEmpty()) {
      sketchContext.put(Query.MAJOR_TYPES, majorTypes);
    }
    sketchContext.put(Query.ALL_DIMENSIONS_FOR_EMPTY, allDimensionsForEmpty(this, false));
    sketchContext.put(Query.ALL_METRICS_FOR_EMPTY, allMetricsForEmpty(this, false));

    SketchQuery quantile = new SketchQuery(
        getDataSource(), getQuerySegmentSpec(), filter, virtualColumns, dimensions, metrics, null, SketchOp.QUANTILE,
        Maps.newHashMap(sketchContext)
    );
    SketchQuery theta = new SketchQuery(
        getDataSource(), getQuerySegmentSpec(), filter, virtualColumns, dimensions, metrics, null, SketchOp.THETA,
        Maps.newHashMap(sketchContext)
    );

    ObjectMapper jsonMapper = segmentWalker.getMapper();
    Map<String, Object> summaryContext = computeOverriddenContext(
        ImmutableMap.<String, Object>of(
            POST_PROCESSING,
            PostProcessingOperators.convert(
                jsonMapper, ImmutableMap.of(
                    "type", "sketch.summary",
                    "round", round,
                    "includeTimeStats", includeTimeStats,
                    "includeCovariance", includeCovariance,
                    "typeDetail", typeDetail
                )
            )
        )
    );

    return new UnionAllQuery(null, Arrays.asList(quantile, theta), false, -1, 2, summaryContext);
  }

  @Override
  public String getType()
  {
    return "summary";
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public boolean isIncludeTimeStats()
  {
    return includeTimeStats;
  }

  @JsonProperty
  public boolean isIncludeCovariance()
  {
    return includeCovariance;
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
        filter,
        round,
        includeTimeStats,
        includeCovariance,
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
        filter,
        round,
        includeTimeStats,
        includeCovariance,
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
        filter,
        round,
        includeTimeStats,
        includeCovariance,
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
        filter,
        round,
        includeTimeStats,
        includeCovariance,
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
        filter,
        round,
        includeTimeStats,
        includeCovariance,
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
        filter,
        round,
        includeTimeStats,
        includeCovariance,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public SummaryQuery withFilter(DimFilter filter)
  {
    return new SummaryQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        metrics,
        filter,
        round,
        includeTimeStats,
        includeCovariance,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder(64)
        .append("SummaryQuery{")
        .append("dataSource='").append(getDataSource()).append('\'')
        .append(", querySegmentSpec=").append(getQuerySegmentSpec());

    if (filter != null) {
      builder.append(", filter=").append(filter);
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
    if (round > 0) {
      builder.append(", round=").append(round);
    }
    builder.append(", includeTimeStats=").append(includeTimeStats);
    builder.append(", includeCovariance=").append(includeCovariance);
    return builder.toString();
  }
}
