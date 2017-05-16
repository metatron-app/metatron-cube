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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.query.UnionAllQuery;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("summary")
public class SummaryQuery extends BaseQuery<Result<Map<String, Object>>>
    implements BaseQuery.RewritingQuery<Result<Map<String, Object>>>,
    Query.DimensionSupport<Result<Map<String, Object>>>
{
  private final DimFilter dimFilter;
  private final List<DimensionSpec> dimensions;
  private final List<VirtualColumn> virtualColumns;

  @JsonCreator
  public SummaryQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.dimFilter = filter;
    this.dimensions = dimensions;
    this.virtualColumns = virtualColumns;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper)
  {
    SketchQuery quantile = new SketchQuery(
        getDataSource(), getQuerySegmentSpec(), dimFilter, dimensions, virtualColumns, 4096, SketchOp.QUANTILE,
        Maps.newHashMap(getContext())
    );
    SketchQuery theta = new SketchQuery(
        getDataSource(), getQuerySegmentSpec(), dimFilter, dimensions, virtualColumns, null, SketchOp.THETA,
        Maps.newHashMap(getContext())
    );
    Map<String, Object> postProcessor = ImmutableMap.<String, Object>of(
        QueryContextKeys.POST_PROCESSING, ImmutableMap.of("type", "sketch.summary")
    );
    Map<String, Object> joinContext = computeOverridenContext(postProcessor);

    return new UnionAllQuery(null, Arrays.asList(quantile, theta), false, -1, -1, -1, joinContext);
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return "summary";
  }

  @Override
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  public SummaryQuery withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new SummaryQuery(getDataSource(), getQuerySegmentSpec(), dimensions, virtualColumns, dimFilter, getContext());
  }

  @Override
  public SummaryQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new SummaryQuery(getDataSource(), getQuerySegmentSpec(), dimensions, virtualColumns, dimFilter, getContext());
  }

  @Override
  public SummaryQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SummaryQuery(getDataSource(), spec, dimensions, virtualColumns, dimFilter, getContext());
  }

  @Override
  public SummaryQuery withDataSource(DataSource dataSource)
  {
    return new SummaryQuery(dataSource, getQuerySegmentSpec(), dimensions, virtualColumns, dimFilter, getContext());
  }

  @Override
  public SummaryQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SummaryQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimensions,
        virtualColumns,
        dimFilter,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public DimFilterSupport<Result<Map<String, Object>>> withDimFilter(DimFilter filter)
  {
    return new SummaryQuery(getDataSource(), getQuerySegmentSpec(), dimensions, virtualColumns, filter, getContext());
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
    if (dimensions != null && !dimensions.isEmpty()) {
      builder.append(", dimensions=").append(dimensions);
    }
    if (virtualColumns != null && !virtualColumns.isEmpty()) {
      builder.append(", virtualColumns=").append(virtualColumns);
    }
    return builder.toString();
  }
}
