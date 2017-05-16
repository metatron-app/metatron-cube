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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
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
@JsonTypeName("sketch")
public class SketchQuery extends BaseQuery<Result<Map<String, Object>>>
    implements Query.DimensionSupport<Result<Map<String, Object>>>
{
  private final List<DimensionSpec> dimensions;
  private final List<VirtualColumn> virtualColumns;
  private final DimFilter filter;
  private final int sketchParam;
  private final SketchOp sketchOp;

  @JsonCreator
  public SketchQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("sketchParam") Integer sketchParam,
      @JsonProperty("sketchOp") SketchOp sketchOp,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.dimensions = dimensions;
    this.virtualColumns = virtualColumns;
    this.filter = filter;
    this.sketchOp = sketchOp == null ? SketchOp.THETA : sketchOp;
    this.sketchParam = sketchParam == null ? this.sketchOp.defaultParam() : this.sketchOp.normalize(sketchParam);
    if (filter != null) {
      Preconditions.checkArgument(filter.optimize().toFilter().supportsBitmap());
    }
  }

  @Override
  public boolean hasFilters()
  {
    return filter != null;
  }

  @Override
  public String getType()
  {
    return "sketch";
  }

  @Override
  public SketchQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SketchQuery(
        getDataSource(),
        spec,
        filter, dimensions, virtualColumns,
        sketchParam,
        sketchOp,
        getContext()
    );
  }

  @Override
  public SketchQuery withDataSource(DataSource dataSource)
  {
    return new SketchQuery(
        dataSource,
        getQuerySegmentSpec(),
        filter,
        dimensions,
        virtualColumns,
        sketchParam,
        sketchOp,
        getContext()
    );
  }

  @Override
  public SketchQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new SketchQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        dimensions,
        virtualColumns,
        sketchParam,
        sketchOp,
        computeOverridenContext(contextOverrides)
    );
  }

  @Override
  public SketchQuery withDimFilter(DimFilter filter)
  {
    return new SketchQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        dimensions,
        virtualColumns,
        sketchParam,
        sketchOp,
        getContext()
    );
  }

  @Override
  public DimFilter getDimFilter()
  {
    return filter;
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  public SketchQuery withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new SketchQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        dimensions,
        virtualColumns,
        sketchParam,
        sketchOp,
        getContext()
    );
  }

  @Override
  public SketchQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new SketchQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        dimensions,
        virtualColumns,
        sketchParam,
        sketchOp,
        getContext()
    );
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  public int getSketchParam()
  {
    return sketchParam;
  }

  @JsonProperty
  public SketchOp getSketchOp()
  {
    return sketchOp;
  }

  @Override
  public String toString()
  {
    return "SketchQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", sketchOp=" + sketchOp +
           ", dimensions=" + dimensions +
           ", virtualColumns=" + virtualColumns +
           ", filter=" + filter +
           ", sketchParam=" + sketchParam +
           toString(POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT) +
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

    SketchQuery that = (SketchQuery) o;

    if (!Objects.equals(dimensions, that.dimensions)) {
      return false;
    }
    if (!Objects.equals(virtualColumns, that.virtualColumns)) {
      return false;
    }
    if (!Objects.equals(filter, that.filter)) {
      return false;
    }
    return sketchOp == that.sketchOp && sketchParam == that.sketchParam;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + Objects.hashCode(dimensions);
    result = 31 * result + Objects.hashCode(virtualColumns);
    result = 31 * result + Objects.hashCode(filter);
    result = 31 * result + sketchOp.ordinal();
    result = 31 * result + sketchParam;
    return result;
  }
}
