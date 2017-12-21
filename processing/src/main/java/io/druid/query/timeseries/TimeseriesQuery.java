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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.LateralViewSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("timeseries")
public class TimeseriesQuery extends BaseQuery<Result<TimeseriesResultValue>>
  implements Query.AggregationsSupport<Result<TimeseriesResultValue>>
{
  private final DimFilter dimFilter;
  private final Granularity granularity;
  private final List<VirtualColumn> virtualColumns;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;
  private final List<String> outputColumns;
  private final LateralViewSpec lateralView;

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
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("lateralView") LateralViewSpec lateralView,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.aggregatorSpecs = aggregatorSpecs == null ? ImmutableList.<AggregatorFactory>of() : aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;
    this.outputColumns = outputColumns;
    this.lateralView = lateralView;

    Queries.verifyAggregations(Arrays.<String>asList(), this.aggregatorSpecs, this.postAggregatorSpecs);
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null || super.hasFilters();
  }

  @Override
  public String getType()
  {
    return Query.TIMESERIES;
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @Override
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
  }

  @Override
  public List<DimensionSpec> getDimensions()
  {
    return ImmutableList.of();
  }

  @Override
  @JsonProperty("virtualColumns")
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty("aggregations")
  public List<AggregatorFactory> getAggregatorSpecs()
  {
    return aggregatorSpecs;
  }

  @JsonProperty("postAggregations")
  public List<PostAggregator> getPostAggregatorSpecs()
  {
    return postAggregatorSpecs;
  }

  @JsonProperty("outputColumns")
  public List<String> getOutputColumns()
  {
    return outputColumns;
  }

  @JsonProperty("lateralView")
  public LateralViewSpec getLateralView()
  {
    return lateralView;
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
        outputColumns,
        lateralView,
        computeOverridenContext(contextOverrides)
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
        outputColumns,
        lateralView,
        getContext()
    );
  }

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
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public AggregationsSupport<Result<TimeseriesResultValue>> withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    Preconditions.checkArgument(dimensions == null || dimensions.isEmpty());
    return this;
  }

  @Override
  public AggregationsSupport<Result<TimeseriesResultValue>> withVirtualColumns(List<VirtualColumn> virtualColumns)
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
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "TimeseriesQuery{" +
        "dataSource='" + getDataSource() + '\'' +
        ", querySegmentSpec=" + getQuerySegmentSpec() +
        ", descending=" + isDescending() +
        ", dimFilter=" + dimFilter +
        ", granularity='" + granularity + '\'' +
        ", virtualColumns=" + virtualColumns +
        ", aggregatorSpecs=" + aggregatorSpecs +
        ", postAggregatorSpecs=" + postAggregatorSpecs +
        ", outputColumns=" + outputColumns +
        ", lateralView=" + lateralView +
        ", context=" + getContext() +
        '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    TimeseriesQuery that = (TimeseriesQuery) o;

    if (virtualColumns != null ? !virtualColumns.equals(that.virtualColumns) : that.virtualColumns != null) {
      return false;
    }
    if (aggregatorSpecs != null ? !aggregatorSpecs.equals(that.aggregatorSpecs) : that.aggregatorSpecs != null)
      return false;
    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) return false;
    if (granularity != null ? !granularity.equals(that.granularity) : that.granularity != null) return false;
    if (postAggregatorSpecs != null ? !postAggregatorSpecs.equals(that.postAggregatorSpecs) : that.postAggregatorSpecs != null)
      return false;
    if (outputColumns != null ? !outputColumns.equals(that.outputColumns) : that.outputColumns != null)
      return false;
    if (lateralView != null ? !lateralView.equals(that.lateralView) : that.lateralView != null)
      return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (virtualColumns != null ? virtualColumns.hashCode() : 0);
    result = 31 * result + (aggregatorSpecs != null ? aggregatorSpecs.hashCode() : 0);
    result = 31 * result + (postAggregatorSpecs != null ? postAggregatorSpecs.hashCode() : 0);
    result = 31 * result + (outputColumns != null ? outputColumns.hashCode() : 0);
    result = 31 * result + (lateralView != null ? lateralView.hashCode() : 0);
    return result;
  }
}
