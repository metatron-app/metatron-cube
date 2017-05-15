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
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("summary")
public class SummaryQuery extends BaseQuery<Result<Map<String, Object>>>
    implements BaseQuery.RewritingQuery<Result<Map<String, Object>>>,
    Query.DimFilterSupport<Result<Map<String, Object>>>
{
  private final DimFilter dimFilter;
  private final List<String> columns;

  @JsonCreator
  public SummaryQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("descending") boolean descending,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, context);
    this.dimFilter = filter;
    this.columns = columns;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper)
  {
    SketchQuery quantile = new SketchQuery(
        getDataSource(), getQuerySegmentSpec(), getColumns(), null, dimFilter, 4096, SketchOp.QUANTILE,
        Maps.newHashMap(getContext())
    );
    SketchQuery theta = new SketchQuery(
        getDataSource(), getQuerySegmentSpec(), getColumns(), null, dimFilter, null, SketchOp.THETA,
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
  public List<String> getColumns()
  {
    return columns;
  }

  @Override
  public SummaryQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SummaryQuery(getDataSource(), spec, isDescending(), dimFilter, columns, getContext());
  }

  @Override
  public SummaryQuery withDataSource(DataSource dataSource)
  {
    return new SummaryQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        columns,
        getContext()
    );
  }

  @Override
  public SummaryQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SummaryQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        columns,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public DimFilterSupport<Result<Map<String, Object>>> withDimFilter(DimFilter filter)
  {
    return new SummaryQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        columns,
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
    if (columns != null && !columns.isEmpty()) {
      builder.append(", columns=").append(columns);
    }
    return builder.toString();
  }
}
