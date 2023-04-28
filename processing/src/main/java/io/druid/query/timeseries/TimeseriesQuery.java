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

package io.druid.query.timeseries;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.DataSource;
import io.druid.query.JoinQuery;
import io.druid.query.LateralViewSpec;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.NoopLimitSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("timeseries")
public class TimeseriesQuery extends BaseAggregationQuery
{
  public static Builder builder()
  {
    return new Builder();
  }

  public static TimeseriesQuery countAll(DataSource dataSource)
  {
    return new TimeseriesQuery(
        dataSource,
        null,
        false,
        null,
        null,
        null,
        Arrays.asList(CountAggregatorFactory.of("count")),
        null,
        null,
        null,
        null,
        null,
        Maps.newHashMap()
    );
  }

  @JsonCreator
  public TimeseriesQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("descending") boolean descending,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("having") HavingSpec havingSpec,
      @JsonProperty("limitSpec") LimitSpec limitSpec,
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("lateralView") LateralViewSpec lateralView,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource,
        querySegmentSpec,
        descending,
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        context
    );
  }

  @Override
  public String getType()
  {
    return Query.TIMESERIES;
  }

  @Override
  public List<DimensionSpec> getDimensions()
  {
    return ImmutableList.of();
  }

  @Override
  public TimeseriesQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new TimeseriesQuery(
        getDataSource(),
        querySegmentSpec,
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public Query<Row> withDataSource(DataSource dataSource)
  {
    return new TimeseriesQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public TimeseriesQuery withOverriddenContext(String contextKey, Object contextValue)
  {
    return (TimeseriesQuery) super.withOverriddenContext(contextKey, contextValue);
  }

  @Override
  public TimeseriesQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        computeOverriddenContext(contextOverrides)
    );
  }

  @Override
  public TimeseriesQuery withFilter(DimFilter filter)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public TimeseriesQuery withOutputColumns(List<String> outputColumns)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public TimeseriesQuery withLateralView(LateralViewSpec lateralView)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public TimeseriesQuery withAggregatorSpecs(List<AggregatorFactory> aggregatorSpecs)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public TimeseriesQuery withPostAggregatorSpecs(List<PostAggregator> postAggregatorSpecs)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public TimeseriesQuery withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    Preconditions.checkArgument(GuavaUtils.isNullOrEmpty(dimensions));
    return this;
  }

  @Override
  public TimeseriesQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public TimeseriesQuery withGranularity(Granularity granularity)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public TimeseriesQuery withLimitSpec(LimitSpec limitSpec)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec.isNoop() ? null : limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public TimeseriesQuery withHavingSpec(HavingSpec havingSpec)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public TimeseriesQuery toLocalQuery()
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        null,
        null,
        limitSpec.withNoLocalProcessing(),
        null,
        null,
        computeOverriddenContext(DEFAULT_DATALOCAL_CONTEXT)
    );
  }

  @Override
  public Comparator<Row> getMergeOrdering(List<String> columns)
  {
    return Granularities.isAll(granularity) ? null : super.getMergeOrdering(columns);
  }

  public HistogramQuery toHistogramQuery(DimensionSpec dimensionSpec, Comparator comparator)
  {
    return new HistogramQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getVirtualColumns(),
        dimensionSpec,
        comparator,
        Iterables.getOnlyElement(postAggregatorSpecs),
        getContext()
    );
  }

  public Query<Row> withDescending(boolean descending)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        descending,
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  public static class Builder extends BaseAggregationQuery.Builder<TimeseriesQuery>
  {
    public Builder() { }

    public Builder(BaseAggregationQuery aggregationQuery)
    {
      super(aggregationQuery);
    }

    @Override
    public TimeseriesQuery build()
    {
      return new TimeseriesQuery(
          dataSource,
          querySegmentSpec,
          descending,
          dimFilter,
          granularity,
          virtualColumns,
          aggregatorSpecs,
          postAggregatorSpecs,
          havingSpec,
          buildLimitSpec(),
          outputColumns,
          lateralViewSpec,
          context
      );
    }
  }

  @Override
  public String toString()
  {
    return "TimeseriesQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           (querySegmentSpec == null || querySegmentSpec == QuerySegmentSpec.ETERNITY ? "" : ", querySegmentSpec=" + querySegmentSpec) +
           (Granularities.isAll(granularity) ? "" : granularity) +
           (limitSpec == NoopLimitSpec.INSTANCE ? "" : ", limitSpec=" + limitSpec) +
           (filter == null ? "" : ", filter=" + filter) +
           (virtualColumns.isEmpty() ? "" : ", virtualColumns=" + virtualColumns) +
           (aggregatorSpecs.isEmpty()? "" : ", aggregatorSpecs=" + aggregatorSpecs) +
           (postAggregatorSpecs.isEmpty() ? "" : ", postAggregatorSpecs=" + postAggregatorSpecs) +
           (havingSpec == null ? "" : ", havingSpec=" + havingSpec) +
           (outputColumns == null ? "" : ", outputColumns=" + outputColumns) +
           (lateralView == null ? "" : "lateralView=" + lateralView) +
           toString(POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT, JoinQuery.HASHING) +
           '}';
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return super.append(builder.append(0x00))
                .append(granularity).append(filter)
                .append(virtualColumns).append(aggregatorSpecs).append(postAggregatorSpecs).append(havingSpec)
                .append(limitSpec).append(outputColumns).append(lateralView);
  }
}
