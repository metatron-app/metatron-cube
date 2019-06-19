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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.Intervals;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.ViewDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("selectMeta")
public class SelectMetaQuery extends BaseQuery<Result<SelectMetaResultValue>>
  implements Query.MetricSupport<Result<SelectMetaResultValue>>
{
  public static SelectMetaQuery forQuery(Query source)
  {
    return forQuery(source, false);
  }

  public static SelectMetaQuery forView(ViewDataSource view)
  {
    return new SelectMetaQuery(
        view,
        MultipleIntervalSegmentSpec.of(Intervals.ETERNITY),
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  @SuppressWarnings("unchecked")
  public static SelectMetaQuery forQuery(Query source, boolean bySegment)
  {
    Builder builder = new Builder()
        .setDataSource(source.getDataSource())
        .setQuerySegmentSpec(source.getQuerySegmentSpec())
        .setGranularity(QueryGranularities.ALL)
        .setContext(BaseQuery.copyContextForMeta(source.getContext()))
        .addContext(BaseQuery.BY_SEGMENT, bySegment);

    if (source instanceof VCSupport) {
      builder.setVirtualColumns(((VCSupport) source).getVirtualColumns());
    }
    if (source instanceof Query.FilterSupport) {
      builder.setDimFilter(((FilterSupport) source).getFilter());
    }
    if (source instanceof DimensionSupport) {
      builder.setDimensions(((DimensionSupport) source).getDimensions());
    }
    if (source instanceof MetricSupport) {
      builder.setMetrics(((MetricSupport) source).getMetrics());
    }
    return builder.build();
  }

  public static SelectMetaQuery of(
      DataSource source,
      QuerySegmentSpec querySegmentSpec,
      DimFilter filter,
      Map<String, Object> context
  )
  {
    return new SelectMetaQuery(
        source,
        querySegmentSpec,
        filter,
        Granularities.ALL,
        null,
        null,
        null,
        null,
        context
    );
  }

  private final DimFilter filter;
  private final Granularity granularity;
  private final List<DimensionSpec> dimensions;
  private final List<String> metrics;
  private final List<VirtualColumn> virtualColumns;
  private final PagingSpec pagingSpec;

  @JsonCreator
  public SelectMetaQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("pagingSpec") PagingSpec pagingSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.metrics = metrics == null ? ImmutableList.<String>of() : metrics;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.filter = filter;
    this.granularity = granularity == null ? Granularities.ALL : granularity;
    this.pagingSpec = pagingSpec;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
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

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public PagingSpec getPagingSpec()
  {
    return pagingSpec;
  }

  public PagingOffset getPagingOffset(String identifier)
  {
    return pagingSpec == null ? PagingOffset.none() : pagingSpec.getOffset(identifier, isDescending());
  }

  SelectQuery toBaseQuery()
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        getPagingSpec(),
        null,
        null,
        null,
        getContext()
    );
  }

  @Override
  public String getType()
  {
    return SELECT_META;
  }

  @Override
  public SelectMetaQuery withOverriddenContext(Map<String, Object> contextOverride)
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
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public SelectMetaQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SelectMetaQuery(
        getDataSource(),
        spec,
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
  public SelectMetaQuery withDataSource(DataSource dataSource)
  {
    return new SelectMetaQuery(
        dataSource,
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
  public SelectMetaQuery withFilter(DimFilter filter)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        getGranularity(),
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        getPagingSpec(),
        getContext()
    );
  }

  @Override
  public SelectMetaQuery withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        dimensions,
        getMetrics(),
        getVirtualColumns(),
        getPagingSpec(),
        getContext()
    );
  }

  @Override
  public SelectMetaQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getMetrics(),
        virtualColumns,
        getPagingSpec(),
        getContext()
    );
  }

  @Override
  public SelectMetaQuery withMetrics(List<String> metrics)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        metrics,
        getVirtualColumns(),
        getPagingSpec(),
        getContext()
    );
  }

  public SelectMetaQuery withQueryGranularity(Granularity granularity)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        granularity,
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        getPagingSpec(),
        getContext()
    );
  }

  public SelectMetaQuery withPagingSpec(PagingSpec pagingSpec)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        pagingSpec,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "SelectMetaQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", filter=" + filter +
           ", granularity=" + granularity +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           ", virtualColumns=" + virtualColumns +
           ", pagingSpec=" + pagingSpec +
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

    SelectMetaQuery that = (SelectMetaQuery) o;

    if (!Objects.equals(filter, that.filter)) {
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
    if (!Objects.equals(pagingSpec, that.pagingSpec)) {
      return false;
    }
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
    return result;
  }

  public static class Builder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private DimFilter dimFilter;
    private Granularity granularity;
    private List<VirtualColumn> virtualColumns;
    private List<DimensionSpec> dimensions;
    private List<String> metrics;
    private PagingSpec pagingSpec;

    private Map<String, Object> context = Maps.newHashMap();

    public Builder()
    {
    }

    public Builder(SelectMetaQuery query)
    {
      dataSource = query.getDataSource();
      querySegmentSpec = query.getQuerySegmentSpec();
      dimFilter = query.getFilter();
      granularity = query.getGranularity();
      dimensions = query.getDimensions();
      metrics = query.getMetrics();
      virtualColumns = query.getVirtualColumns();
      context = query.getContext();
    }

    public Builder setDataSource(DataSource dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Builder setDataSource(String dataSource)
    {
      this.dataSource = new TableDataSource(dataSource);
      return this;
    }

    public Builder setDataSource(Query query)
    {
      this.dataSource = QueryDataSource.of(query);
      return this;
    }

    public Builder setInterval(QuerySegmentSpec interval)
    {
      return setQuerySegmentSpec(interval);
    }

    public Builder setInterval(List<Interval> intervals)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(intervals));
    }

    public Builder setInterval(Interval interval)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(interval));
    }

    public Builder setInterval(String interval)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(interval));
    }

    public Builder setQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
    {
      this.querySegmentSpec = querySegmentSpec;
      return this;
    }

    public Builder setDimFilter(DimFilter dimFilter)
    {
      this.dimFilter = dimFilter;
      return this;
    }

    public Builder setGranularity(Granularity granularity)
    {
      this.granularity = granularity;
      return this;
    }

    public Builder addDimension(String column)
    {
      return addDimension(column, column);
    }

    public Builder addDimension(String column, String outputName)
    {
      return addDimension(new DefaultDimensionSpec(column, outputName));
    }

    public Builder addDimension(DimensionSpec dimension)
    {
      if (dimensions == null) {
        dimensions = Lists.newArrayList();
      }
      dimensions.add(dimension);
      return this;
    }

    public Builder setDimensions(List<DimensionSpec> dimensions)
    {
      this.dimensions = Lists.newArrayList(dimensions);
      return this;
    }

    public Builder setDimensions(DimensionSpec... dimensions)
    {
      this.dimensions = Lists.newArrayList(dimensions);
      return this;
    }

    public Builder setVirtualColumns(List<VirtualColumn> virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public Builder setVirtualColumns(VirtualColumn... virtualColumns)
    {
      this.virtualColumns = Arrays.asList(virtualColumns);
      return this;
    }

    public Builder setMetrics(List<String> metrics)
    {
      this.metrics = metrics;
      return this;
    }

    public Builder setPagingSpec(PagingSpec pagingSpec)
    {
      this.pagingSpec = pagingSpec;
      return this;
    }

    public Builder setContext(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public Builder addContext(String key, Object value)
    {
      context.put(key, value);
      return this;
    }

    public SelectMetaQuery build()
    {
      return new SelectMetaQuery(
          dataSource,
          querySegmentSpec,
          dimFilter,
          granularity,
          dimensions,
          metrics,
          virtualColumns,
          pagingSpec,
          context
      );
    }
  }
}
