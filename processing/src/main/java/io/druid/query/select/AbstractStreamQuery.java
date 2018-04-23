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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public abstract class AbstractStreamQuery<T extends Comparable<T>> extends BaseQuery<T>
    implements Query.MetricSupport<T>
{
  private final DimFilter dimFilter;
  private final Granularity granularity;
  private final List<DimensionSpec> dimensions;
  private final List<String> metrics;
  private final List<VirtualColumn> virtualColumns;
  private final String concatString;
  private final int limit;

  public AbstractStreamQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      DimFilter dimFilter,
      Granularity granularity,
      List<DimensionSpec> dimensions,
      List<String> metrics,
      List<VirtualColumn> virtualColumns,
      String concatString,
      int limit,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity == null ? QueryGranularities.ALL : granularity;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.metrics = metrics == null ? ImmutableList.<String>of() : metrics;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.concatString = concatString;
    this.limit = limit > 0 ? limit : -1;
  }

  @JsonProperty("filter")
  @JsonInclude(Include.NON_NULL)
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

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getConcatString()
  {
    return concatString;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
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

  public SelectMetaQuery toMetaQuery(boolean schemaOnly)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        schemaOnly,
        null,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder(64)
        .append(getType()).append('{')
        .append("dataSource='").append(getDataSource()).append('\'')
        .append(", querySegmentSpec=").append(getQuerySegmentSpec())
        .append(", granularity=").append(granularity)
        .append(", limit=").append(limit);

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
    if (concatString != null) {
      builder.append(", concatString=").append(concatString);
    }
    builder.append(toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT));
    return builder.append('}').toString();
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

    AbstractStreamQuery that = (AbstractStreamQuery) o;

    if (!Objects.equals(dimFilter, that.dimFilter)) {
      return false;
    }
    if (!Objects.equals(granularity, that.granularity)) {
      return false;
    }
    if (!Objects.equals(dimensions, that.dimensions)) {
      return false;
    }
    if (!Objects.equals(metrics, that.metrics)) {
      return false;
    }
    if (!Objects.equals(virtualColumns, that.virtualColumns)) {
      return false;
    }
    if (!Objects.equals(concatString, that.concatString)) {
      return false;
    }
    if (limit != that.limit) {
      return false;
    }

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
    result = 31 * result + (concatString != null ? concatString.hashCode() : 0);
    result = 31 * result + limit;
    return result;
  }
}
