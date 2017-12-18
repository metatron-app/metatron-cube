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

package io.druid.query.kmeans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.druid.granularity.QueryGranularities;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;

/**
 */
public class FindNearestQuery extends BaseQuery<CentroidDesc>
{
  private final List<VirtualColumn> virtualColumns;
  private final List<String> metrics;
  private final List<Centroid> centroids;

  public FindNearestQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("centroids") List<Centroid> centroids,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = virtualColumns;
    this.metrics = metrics;
    this.centroids = centroids;
  }

  @Override
  public String getType()
  {
    return "kmeans.nearest";
  }

  @JsonProperty
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public List<Centroid> getCentroids()
  {
    return centroids;
  }

  @Override
  public Query<CentroidDesc> withDataSource(DataSource dataSource)
  {
    return new FindNearestQuery(
        dataSource,
        getQuerySegmentSpec(),
        getVirtualColumns(),
        getMetrics(),
        getCentroids(),
        getContext()
    );
  }

  @Override
  public Query<CentroidDesc> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new FindNearestQuery(
        getDataSource(),
        spec,
        getVirtualColumns(),
        getMetrics(),
        getCentroids(),
        getContext()
    );
  }

  @Override
  public Query<CentroidDesc> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new FindNearestQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getVirtualColumns(),
        getMetrics(),
        getCentroids(),
        computeOverridenContext(contextOverride)
    );
  }

  public StreamQuery asInput()
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        null,
        QueryGranularities.ALL,
        null,
        getMetrics(),
        getVirtualColumns(),
        null,
        -1,
        computeOverridenContext(ImmutableMap.<String, Object>of(ALL_DIMENSIONS_FOR_EMPTY, false))
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
    builder.append(toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT));
    return builder.append('}').toString();
  }
}
