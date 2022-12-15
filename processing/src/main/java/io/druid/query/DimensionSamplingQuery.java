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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 *
 */
@JsonTypeName(Query.DIMENSION_SAMPLING)
public class DimensionSamplingQuery extends BaseQuery<Object[]> implements Query.DimensionSupport<Object[]>
{
  private final DimFilter filter;
  private final List<VirtualColumn> virtualColumns;
  private final List<DimensionSpec> dimensions;
  private final float sampleRatio;

  @JsonCreator
  public DimensionSamplingQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("sampleRatio") float sampleRatio,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.filter = filter;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.dimensions = dimensions;
    this.sampleRatio = sampleRatio;
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(dimensions), "dimensions should not be empty");
    Preconditions.checkArgument(sampleRatio > 0 && sampleRatio <= 0.1f, "invalid sampleRatio [%d]", sampleRatio);
  }

  @Override
  public String getType()
  {
    return DIMENSION_SAMPLING;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public float getSampleRatio()
  {
    return sampleRatio;
  }

  @Override
  public Comparator<Object[]> getMergeOrdering(List<String> columns)
  {
    return null;
  }

  @Override
  public DimensionSamplingQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new DimensionSamplingQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        dimensions,
        sampleRatio,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public DimensionSamplingQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new DimensionSamplingQuery(
        getDataSource(),
        spec,
        filter,
        virtualColumns,
        dimensions,
        sampleRatio,
        getContext()
    );
  }

  @Override
  public DimensionSamplingQuery withDataSource(DataSource dataSource)
  {
    return new DimensionSamplingQuery(
        dataSource,
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        dimensions,
        sampleRatio,
        getContext()
    );
  }

  @Override
  public DimensionSupport<Object[]> withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new DimensionSamplingQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        dimensions,
        sampleRatio,
        context
    );
  }

  @Override
  public DimensionSupport<Object[]> withFilter(DimFilter filter)
  {
    return new DimensionSamplingQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        dimensions,
        sampleRatio,
        context
    );
  }

  @Override
  public DimensionSupport<Object[]> withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new DimensionSamplingQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        dimensions,
        sampleRatio,
        context
    );
  }

  @Override
  public String toString()
  {
    return String.format(
        "DimensionSamplingQuery{dataSource='%s'%s, dimensions=%s%s%s, sampleRatio=%.3f}",
        getDataSource(),
        getQuerySegmentSpec() == null ? "" : ", querySegmentSpec=" + getQuerySegmentSpec(),
        dimensions,
        filter == null ? "" : ", filter=" + filter,
        virtualColumns.isEmpty() ? "" : ", virtualColumns=" + virtualColumns,
        sampleRatio
    );
  }
}
