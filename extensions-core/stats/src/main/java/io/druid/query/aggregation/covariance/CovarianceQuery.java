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

package io.druid.query.aggregation.covariance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularities;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.corr.PearsonAggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("covariance")
public class CovarianceQuery extends BaseQuery<Result<Map<String, Object>>>
    implements Query.RewritingQuery<Result<Map<String, Object>>>,
    Query.FilterSupport<Result<Map<String, Object>>>
{
  private final DimFilter filter;
  private final String column;
  private final List<String> excludes;
  private final List<VirtualColumn> virtualColumns;

  @JsonCreator
  public CovarianceQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("column") String column,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("excludes") List<String>  excludes,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.filter = filter;
    this.column = Preconditions.checkNotNull(column, "column cannot be null");
    this.excludes = excludes == null ? ImmutableList.<String>of() : excludes;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    Map<String, ValueDesc> majorTypes = QueryUtils.toMajorType(QueryUtils.analyzeTypes(segmentWalker, this));

    List<AggregatorFactory> aggregators = Lists.newArrayList();
    for (Map.Entry<String, ValueDesc> entry : majorTypes.entrySet()) {
      String target = entry.getKey();
      if (column.equals(target) || excludes.contains(target)) {
        continue;
      }
      if (entry.getValue().isPrimitiveNumeric()) {
        aggregators.add(new PearsonAggregatorFactory(target, column, target, null, "double"));
      }
    }
    Map<String, Object> postProcessor = ImmutableMap.<String, Object>of(
        Query.POST_PROCESSING, new CovariancePostProcessor()
    );
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        false,
        null,
        Granularities.ALL,
        virtualColumns,
        aggregators,
        ImmutableList.<PostAggregator>of(),
        null,
        null,
        null,
        null,
        computeOverriddenContext(postProcessor)
    );
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
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public String getColumn()
  {
    return column;
  }

  @Override
  public CovarianceQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new CovarianceQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        column,
        filter,
        excludes,
        getContext()
    );
  }

  @Override
  public CovarianceQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new CovarianceQuery(
        getDataSource(),
        spec,
        virtualColumns,
        column,
        filter,
        excludes,
        getContext()
    );
  }

  @Override
  public CovarianceQuery withDataSource(DataSource dataSource)
  {
    return new CovarianceQuery(
        dataSource,
        getQuerySegmentSpec(),
        virtualColumns,
        column,
        filter,
        excludes,
        getContext()
    );
  }

  @Override
  public CovarianceQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new CovarianceQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        column,
        filter,
        excludes,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public CovarianceQuery withFilter(DimFilter filter)
  {
    return new CovarianceQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        column,
        filter,
        excludes,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder(64)
        .append("CovarianceQuery{")
        .append("dataSource='").append(getDataSource()).append('\'')
        .append(", querySegmentSpec=").append(getQuerySegmentSpec());

    if (filter != null) {
      builder.append(", filter=").append(filter);
    }
    if (virtualColumns != null && !virtualColumns.isEmpty()) {
      builder.append(", virtualColumns=").append(virtualColumns);
    }
    builder.append(", column=").append(column);
    if (excludes != null && !excludes.isEmpty()) {
      builder.append(", excludes=").append(excludes);
    }
    builder.append('}');
    return builder.toString();
  }
}
