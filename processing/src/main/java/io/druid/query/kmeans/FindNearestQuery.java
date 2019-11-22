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

package io.druid.query.kmeans;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("kmeans.nearest")
public class FindNearestQuery extends BaseQuery<CentroidDesc> implements Query.FilterSupport<CentroidDesc>
{
  private final List<VirtualColumn> virtualColumns;
  private final DimFilter filter;
  private final List<String> metrics;
  private final List<Centroid> centroids;
  private final String measure;
  private final double maxDistance;

  @JsonCreator
  public FindNearestQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("centroids") List<Centroid> centroids,
      @JsonProperty("measure") String measure,
      @JsonProperty("maxDistance") double maxDistance,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = virtualColumns;
    this.filter = filter;
    this.metrics = metrics;
    this.measure = measure;
    this.centroids = centroids;
    this.maxDistance = maxDistance;
  }

  @Override
  public String getType()
  {
    return "kmeans.nearest";
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
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<Centroid> getCentroids()
  {
    return centroids;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public String getMeasure()
  {
    return measure;
  }

  @JsonProperty
  public double getMaxDistance()
  {
    return maxDistance;
  }

  @Override
  public Query<CentroidDesc> withDataSource(DataSource dataSource)
  {
    return new FindNearestQuery(
        dataSource,
        getQuerySegmentSpec(),
        getFilter(),
        getVirtualColumns(),
        getMetrics(),
        getCentroids(),
        getMeasure(),
        getMaxDistance(),
        getContext()
    );
  }

  @Override
  public Query<CentroidDesc> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new FindNearestQuery(
        getDataSource(),
        spec,
        getFilter(),
        getVirtualColumns(),
        getMetrics(),
        getCentroids(),
        getMeasure(),
        getMaxDistance(),
        getContext()
    );
  }

  @Override
  public Query<CentroidDesc> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new FindNearestQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getVirtualColumns(),
        getMetrics(),
        getCentroids(),
        getMeasure(),
        getMaxDistance(),
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public VCSupport<CentroidDesc> withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new FindNearestQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        virtualColumns,
        getMetrics(),
        getCentroids(),
        getMeasure(),
        getMaxDistance(),
        getContext()
    );
  }

  @Override
  public FilterSupport<CentroidDesc> withFilter(DimFilter filter)
  {
    return new FindNearestQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        getVirtualColumns(),
        getMetrics(),
        getCentroids(),
        getMeasure(),
        getMaxDistance(),
        getContext()
    );
  }

  public StreamQuery asInput()
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getMetrics(),
        getVirtualColumns(),
        null,
        null,
        null,
        null,
        computeOverriddenContext(ImmutableMap.<String, Object>of(ALL_DIMENSIONS_FOR_EMPTY, false))
    );
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder(64);
    builder.append(getType()).append('{')
           .append("dataSource='").append(getDataSource()).append('\'')
           .append(", querySegmentSpec=").append(getQuerySegmentSpec())
           .append(", metrics=").append(getMetrics())
           .append(", centroids=").append(getCentroids());

    if (virtualColumns != null && !virtualColumns.isEmpty()) {
      builder.append(", virtualColumns=").append(virtualColumns);
    }
    if (metrics != null && !metrics.isEmpty()) {
      builder.append(", metrics=").append(metrics);
    }
    if (measure != null) {
      builder.append(", measure=").append(measure);
    }
    builder.append(toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT));
    return builder.append('}').toString();
  }
}
