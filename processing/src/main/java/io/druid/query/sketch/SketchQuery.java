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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("sketch")
public class SketchQuery extends BaseQuery<Object[]>
    implements Query.MetricSupport<Object[]>, Query.ArrayOutput
{
  public static SketchQuery theta(String dataSource, Interval interval)
  {
    return new SketchQuery(
        TableDataSource.of(dataSource),
        MultipleIntervalSegmentSpec.of(interval),
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  private final List<DimensionSpec> dimensions;
  private final List<String> metrics;
  private final List<VirtualColumn> virtualColumns;
  private final DimFilter filter;
  private final int sketchParam;
  private final SketchOp sketchOp;

  @JsonCreator
  public SketchQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("sketchParam") Integer sketchParam,
      @JsonProperty("sketchOp") SketchOp sketchOp,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.metrics = metrics == null ? ImmutableList.<String>of() : metrics;
    this.filter = filter;
    this.sketchOp = sketchOp == null ? SketchOp.THETA : sketchOp;
    this.sketchParam = sketchParam == null ? 0 : this.sketchOp.normalize(sketchParam);
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
        filter,
        virtualColumns,
        dimensions,
        metrics,
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
        virtualColumns,
        dimensions,
        metrics,
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
        virtualColumns,
        dimensions,
        metrics,
        sketchParam,
        sketchOp,
        computeOverriddenContext(contextOverrides)
    );
  }

  @Override
  public SketchQuery withFilter(DimFilter filter)
  {
    return new SketchQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        dimensions,
        metrics,
        sketchParam,
        sketchOp,
        getContext()
    );
  }

  @Override
  public SketchQuery withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new SketchQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        dimensions,
        metrics,
        sketchParam,
        sketchOp,
        getContext()
    );
  }

  @Override
  public SketchQuery withMetrics(List<String> metrics)
  {
    return new SketchQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        dimensions,
        metrics,
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
        virtualColumns,
        dimensions,
        metrics,
        sketchParam,
        sketchOp,
        getContext()
    );
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
  @JsonInclude(Include.NON_NULL)
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public List<String> getMetrics()
  {
    return metrics;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
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

  public int getSketchParamWithDefault()
  {
    return sketchParam == 0 ? sketchOp.defaultParam() : sketchParam;
  }

  @Override
  public Comparator<Object[]> getMergeOrdering()
  {
    return GuavaUtils.allEquals();
  }

  @Override
  public String toString()
  {
    return "SketchQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", sketchOp=" + sketchOp +
           ", sketchParam=" + sketchParam +
           ", virtualColumns=" + virtualColumns +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
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
    return sketchOp == that.sketchOp && Objects.equals(sketchParam, that.sketchParam);
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + Objects.hashCode(dimensions);
    result = 31 * result + Objects.hashCode(virtualColumns);
    result = 31 * result + Objects.hashCode(filter);
    result = 31 * result + sketchOp.ordinal();
    result = 31 * result + Objects.hashCode(sketchParam);
    return result;
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    if (dimensions.isEmpty() && allDimensionsForEmpty() || metrics.isEmpty() && allMetricsForEmpty()) {
      return null;
    }
    return GuavaUtils.concat(
        Row.TIME_COLUMN_NAME, GuavaUtils.concat(DimensionSpecs.toOutputNames(dimensions), metrics)
    );
  }
}
