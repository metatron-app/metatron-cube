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

package io.druid.query.timeseries;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.guava.GuavaUtils;
import io.druid.granularity.Granularity;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.DataSource;
import io.druid.query.LateralViewSpec;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("timeseries")
public class TimeseriesQuery extends BaseAggregationQuery<Result<TimeseriesResultValue>>
{
  @JsonCreator
  public TimeseriesQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("descending") boolean descending,
      @JsonProperty("filter") DimFilter dimFilter,
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
        dimFilter,
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

  public boolean isSkipEmptyBuckets()
  {
    return getContextBoolean("skipEmptyBuckets", false);
  }

  @Override
  public TimeseriesQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new TimeseriesQuery(
        getDataSource(),
        querySegmentSpec,
        isDescending(),
        dimFilter,
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
  public Query<Result<TimeseriesResultValue>> withDataSource(DataSource dataSource)
  {
    return new TimeseriesQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
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
  public TimeseriesQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
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
  public TimeseriesQuery withDimFilter(DimFilter dimFilter)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
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
        dimFilter,
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
        dimFilter,
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
        dimFilter,
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
        dimFilter,
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
        dimFilter,
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
        dimFilter,
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
        dimFilter,
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
  public TimeseriesQuery withHavingSpec(HavingSpec havingSpec)
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
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
  public TimeseriesQuery removePostActions()
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        null,
        null,
        limitSpec.withNoProcessing(),
        null,
        null,
        computeOverriddenContext(removeContext(POST_PROCESSING))
    );
  }

  @Override
  public String toString()
  {
    return "TimeseriesQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", descending=" + isDescending() +
           ", granularity=" + granularity +
           (limitSpec == null ? "" : ", limitSpec=" + limitSpec) +
           (dimFilter == null ? "" : ", dimFilter=" + dimFilter) +
           (virtualColumns.isEmpty() ? "" : ", virtualColumns=" + virtualColumns) +
           (aggregatorSpecs.isEmpty()? "" : ", aggregatorSpecs=" + aggregatorSpecs) +
           (postAggregatorSpecs.isEmpty() ? "" : ", postAggregatorSpecs=" + postAggregatorSpecs) +
           (havingSpec == null ? "" : ", havingSpec=" + havingSpec) +
           (outputColumns == null ? "" : ", outputColumns=" + outputColumns) +
           (lateralView == null ? "" : "lateralView=" + lateralView) +
           ", context=" + getContext() +
           '}';
  }

  @Override
  public Sequence<Object[]> array(Sequence<Result<TimeseriesResultValue>> sequence)
  {
    final String[] columns = Preconditions.checkNotNull(estimatedOutputColumns()).toArray(new String[0]);
    return Sequences.map(
        sequence,
        new Function<Result<TimeseriesResultValue>, Object[]>()
        {
          @Override
          public Object[] apply(Result<TimeseriesResultValue> input)
          {
            final TimeseriesResultValue value = input.getValue();
            final Object[] array = new Object[columns.length];
            for (int i = 0; i < columns.length; i++) {
              array[i] = value.getMetric(columns[i]);
            }
            return array;
          }
        }
    );
  }
}
