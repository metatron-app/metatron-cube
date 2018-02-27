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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.granularity.Granularities;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class SchemaQuery extends BaseQuery<Result<SelectMetaResultValue>>
    implements Query.RewritingQuery<Result<SelectMetaResultValue>>
{
  private final List<DimensionSpec> dimensions;
  private final List<String> metrics;
  private final List<VirtualColumn> virtualColumns;

  @JsonCreator
  public SchemaQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.virtualColumns = virtualColumns;
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
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

  @Override
  public String getType()
  {
    return Query.SCHEMA;
  }

  @Override
  public SelectMetaQuery rewriteQuery(
      QuerySegmentWalker segmentWalker, QueryConfig queryConfig, ObjectMapper jsonMapper
  )
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        null,
        Granularities.ALL,
        dimensions,
        metrics,
        virtualColumns,
        true,
        null,
        getContext()
    );
  }

  @Override
  public SchemaQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SchemaQuery(getDataSource(), spec, getDimensions(), getMetrics(), getVirtualColumns(), getContext());
  }

  @Override
  public SchemaQuery withDataSource(DataSource dataSource)
  {
    return new SchemaQuery(
        dataSource,
        getQuerySegmentSpec(),
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        getContext()
    );
  }

  @Override
  public SchemaQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SchemaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public String toString()
  {
    return "SchemaQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           ", virtualColumns=" + virtualColumns +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    SchemaQuery that = (SchemaQuery) o;

    if (!Objects.equals(dimensions, that.dimensions)) {
      return false;
    }
    if (!Objects.equals(metrics, that.metrics)) {
      return false;
    }
    if (!Objects.equals(virtualColumns, that.virtualColumns)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
    result = 31 * result + (virtualColumns != null ? virtualColumns.hashCode() : 0);
    return result;
  }
}
