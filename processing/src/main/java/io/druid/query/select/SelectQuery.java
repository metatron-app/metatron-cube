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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.LateralViewSpec;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
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
    implements Query.MetricSupport<Result<SelectResultValue>>,
    Query.ArrayOutputSupport<Result<SelectResultValue>>,
    Query.RewritingQuery<Result<SelectResultValue>>
{
  private final DimFilter filter;
  private final Granularity granularity;
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
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("granularity") Granularity granularity,
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
    this.filter = filter;
    this.granularity = granularity == null ? QueryGranularities.ALL : granularity;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.metrics = metrics == null ? ImmutableList.<String>of() : metrics;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
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

  public SelectMetaQuery toMetaQuery(boolean schemaOnly)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        getPagingSpec(),
        getContext()
    );
  }

  @Override
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    final PagingSpec pagingSpec = getPagingSpec();
    final int threshold = pagingSpec.getThreshold();
    final int maxthreshold = queryConfig.getSelect().getMaxThreshold();
    if (threshold < 0 || threshold > maxthreshold) {
      return withPagingSpec(pagingSpec.withThreshold(maxthreshold));
    }
    return this;
  }

  @Override
  public String getType()
  {
    return Query.SELECT;
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
  public Granularity getGranularity()
  {
    return granularity;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
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
  @JsonInclude(Include.NON_NULL)
  public String getConcatString()
  {
    return concatString;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getMetrics()
  {
    return metrics;
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
  public List<String> getOutputColumns()
  {
    return outputColumns;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public LateralViewSpec getLateralView()
  {
    return lateralView;
  }

  public PagingOffset getPagingOffset(String identifier)
  {
    return pagingSpec.getOffset(identifier, isDescending());
  }

  @Override
  public SelectQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new SelectQuery(
        getDataSource(),
        querySegmentSpec,
        isDescending(),
        filter,
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
        filter,
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
  public SelectQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        pagingSpec,
        concatString,
        outputColumns,
        lateralView,
        computeOverriddenContext(contextOverrides)
    );
  }

  public SelectQuery withPagingSpec(PagingSpec pagingSpec)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
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
  public SelectQuery withFilter(DimFilter filter)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
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
  public SelectQuery withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
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
  public SelectQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
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
        filter,
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
        filter,
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
  public SelectQuery toLocalQuery()
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        pagingSpec,
        concatString,
        null,
        null,
        computeOverriddenContext(contextRemover(POST_PROCESSING))
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

    if (filter != null) {
      builder.append(", filter=").append(filter);
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

    if (!Objects.equals(filter, that.filter)) return false;
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
    result = 31 * result + (filter != null ? filter.hashCode() : 0);
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

  @Override
  public List<String> estimatedOutputColumns()
  {
    if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
      return outputColumns;
    }
    if (dimensions.isEmpty() && allDimensionsForEmpty() || metrics.isEmpty() && allMetricsForEmpty()) {
      return null;
    }
    return GuavaUtils.concat(DimensionSpecs.toOutputNames(dimensions), metrics);
  }

  @Override
  public Sequence<Object[]> array(Sequence<Result<SelectResultValue>> sequence)
  {
    final List<String> outputColumns = estimatedOutputColumns();
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(outputColumns));

    return Sequences.explode(
        sequence, new Function<Result<SelectResultValue>, Sequence<Object[]>>()
        {
          private final String[] columns = outputColumns.toArray(new String[0]);

          @Override
          public Sequence<Object[]> apply(Result<SelectResultValue> input)
          {
            final List<Object[]> list = Lists.newArrayList();
            for (EventHolder holder : input.getValue()) {
              Map<String, Object> event = holder.getEvent();
              final Object[] array = new Object[columns.length];
              for (int i = 0; i < columns.length; i++) {
                array[i] = event.get(columns[i]);
              }
              list.add(array);
            }
            return Sequences.simple(list);
          }
        }
    );
  }
}
