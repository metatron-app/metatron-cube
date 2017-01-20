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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.LateralViewSpec;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("select")
public class SelectQuery extends BaseQuery<Result<SelectResultValue>>
    implements Query.ViewSupport<Result<SelectResultValue>>
{
  private final DimFilter dimFilter;
  private final QueryGranularity granularity;
  private final List<DimensionSpec> dimensions;
  private final List<String> metrics;
  private final List<VirtualColumn> virtualColumns;
  private final PagingSpec pagingSpec;
  private final String concatString;
  private final List<String> outputColumns;
  private final LateralViewSpec lateralView;

  @JsonCreator
  public SelectQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("descending") boolean descending,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("pagingSpec") PagingSpec pagingSpec,
      @JsonProperty("concatString") String concatString,
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("lateralView") LateralViewSpec lateralView,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity == null ? QueryGranularities.ALL : granularity;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.metrics = metrics == null ? ImmutableList.<String>of() : metrics;
    this.virtualColumns = virtualColumns;
    this.lateralView = lateralView;
    this.outputColumns = outputColumns;
    this.pagingSpec = pagingSpec == null ? PagingSpec.GET_ALL : pagingSpec;
    this.concatString = concatString;

    Preconditions.checkArgument(checkPagingSpec(pagingSpec, descending), "invalid pagingSpec");
  }

  private boolean checkPagingSpec(PagingSpec pagingSpec, boolean descending)
  {
    if (pagingSpec == null) {
      return true;
    }
    for (Integer value : pagingSpec.getPagingIdentifiers().values()) {
      if (descending ^ (value < 0)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null || ViewSupportHelper.hasFilter(getDataSource());
  }

  @Override
  public String getType()
  {
    return Query.SELECT;
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
  public QueryGranularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public PagingSpec getPagingSpec()
  {
    return pagingSpec;
  }

  @JsonProperty
  public String getConcatString()
  {
    return concatString;
  }

  @JsonProperty
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public List<String> getOutputColumns()
  {
    return outputColumns;
  }

  @JsonProperty
  public LateralViewSpec getLateralView()
  {
    return lateralView;
  }

  public PagingOffset getPagingOffset(String identifier)
  {
    return pagingSpec.getOffset(identifier, isDescending());
  }

  public SelectQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new SelectQuery(
        getDataSource(),
        querySegmentSpec,
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        pagingSpec,
        concatString,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public SelectQuery withDataSource(DataSource dataSource)
  {
    return new SelectQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        pagingSpec,
        concatString,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  public SelectQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        pagingSpec,
        concatString,
        outputColumns,
        lateralView,
        computeOverridenContext(contextOverrides)
    );
  }

  public SelectQuery withPagingSpec(PagingSpec pagingSpec)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        pagingSpec,
        concatString,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public SelectQuery withDimFilter(DimFilter dimFilter)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        pagingSpec,
        concatString,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public SelectQuery withDimensions(List<DimensionSpec> dimensions)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        pagingSpec,
        concatString,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public SelectQuery withMetrics(List<String> metrics)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        pagingSpec,
        concatString,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  public SelectQuery withConcatString(String concatString)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        pagingSpec,
        concatString,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder(64)
        .append("SelectQuery{")
        .append("dataSource='").append(getDataSource()).append('\'')
        .append(", querySegmentSpec=").append(getQuerySegmentSpec())
        .append(", descending=").append(isDescending())
        .append(", granularity=").append(granularity);

    if (dimFilter != null) {
      builder.append(", dimFilter=").append(dimFilter);
    }
    if (dimensions != null && !dimensions.isEmpty()) {
      builder.append(", dimensions=").append(dimensions);
    }
    if (metrics != null && !metrics.isEmpty()) {
      builder.append(", metrics=").append(metrics);
    }
    if (virtualColumns != null && !virtualColumns.isEmpty()) {
      builder.append(", virtualColumns=").append(virtualColumns);
    }
    if (pagingSpec != null && !pagingSpec.equals(PagingSpec.GET_ALL)) {
      builder.append(", pagingSpec=").append(pagingSpec);
    }
    if (concatString != null) {
      builder.append(", concatString=").append(concatString);
    }
    if (outputColumns != null) {
      builder.append(", outputColumns=").append(outputColumns);
    }
    if (lateralView != null) {
      builder.append(", lateralView=").append(lateralView);
    }
    builder.append(toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT));
    return builder.append('}').toString();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    SelectQuery that = (SelectQuery) o;

    if (!Objects.equals(dimFilter, that.dimFilter)) return false;
    if (!Objects.equals(granularity, that.granularity)) return false;
    if (!Objects.equals(dimensions, that.dimensions)) return false;
    if (!Objects.equals(metrics, that.metrics)) return false;
    if (!Objects.equals(virtualColumns, that.virtualColumns)) return false;
    if (!Objects.equals(pagingSpec, that.pagingSpec)) return false;
    if (!Objects.equals(concatString, that.concatString)) return false;
    if (!Objects.equals(outputColumns, that.outputColumns)) return false;
    if (!Objects.equals(lateralView, that.lateralView)) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
    result = 31 * result + (virtualColumns != null ? virtualColumns.hashCode() : 0);
    result = 31 * result + (pagingSpec != null ? pagingSpec.hashCode() : 0);
    result = 31 * result + (concatString != null ? concatString.hashCode() : 0);
    result = 31 * result + (outputColumns != null ? outputColumns.hashCode() : 0);
    result = 31 * result + (lateralView != null ? lateralView.hashCode() : 0);
    return result;
  }
}
