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
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.NoopLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderedLimitSpec;
import io.druid.query.ordering.Direction;
import io.druid.query.select.Schema;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public abstract class BaseAggregationQuery extends BaseQuery<Row>
    implements Query.AggregationsSupport<Row>, Query.ArrayOutputSupport<Row>, Query.OrderingSupport<Row>
{
  public static final String SORT_ON_TIME = "groupby.sort.on.time";

  protected final LimitSpec limitSpec;
  protected final HavingSpec havingSpec;
  protected final LateralViewSpec lateralView;
  protected final DimFilter filter;
  protected final Granularity granularity;
  protected final List<VirtualColumn> virtualColumns;
  protected final List<AggregatorFactory> aggregatorSpecs;
  protected final List<PostAggregator> postAggregatorSpecs;
  protected final List<String> outputColumns;

  @JsonCreator
  public BaseAggregationQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("descending") boolean descending,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("having") HavingSpec havingSpec,
      @JsonProperty("limitSpec") LimitSpec limitSpec,
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("lateralView") LateralViewSpec lateralView,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, context);
    this.filter = filter;
    this.granularity = Preconditions.checkNotNull(granularity, "Must specify a granularity");
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.aggregatorSpecs = aggregatorSpecs == null ? ImmutableList.<AggregatorFactory>of() : aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;
    this.havingSpec = havingSpec;
    this.lateralView = lateralView;
    this.limitSpec = limitSpec == null ? NoopLimitSpec.INSTANCE : limitSpec;
    this.outputColumns = outputColumns;
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
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  @JsonProperty("aggregations")
  @JsonInclude(Include.NON_EMPTY)
  public List<AggregatorFactory> getAggregatorSpecs()
  {
    return aggregatorSpecs;
  }

  @Override
  @JsonProperty("postAggregations")
  @JsonInclude(Include.NON_EMPTY)
  public List<PostAggregator> getPostAggregatorSpecs()
  {
    return postAggregatorSpecs;
  }

  @JsonProperty("having")
  @JsonInclude(Include.NON_NULL)
  public HavingSpec getHavingSpec()
  {
    return havingSpec;
  }

  @JsonProperty
  public LimitSpec getLimitSpec()
  {
    return limitSpec;
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

  public abstract BaseAggregationQuery withGranularity(Granularity granularity);

  public abstract BaseAggregationQuery withLimitSpec(LimitSpec limitSpec);

  public abstract BaseAggregationQuery withHavingSpec(HavingSpec havingSpec);

  public abstract BaseAggregationQuery withOutputColumns(List<String> outputColumns);

  public abstract BaseAggregationQuery withLateralView(LateralViewSpec lateralView);

  @Override
  public List<OrderByColumnSpec> getResultOrdering()
  {
    return limitSpec.getColumns();
  }

  @Override
  public BaseAggregationQuery withResultOrdering(List<OrderByColumnSpec> orderingSpecs)
  {
    return withLimitSpec(limitSpec.withOrderingSpec(orderingSpecs));
  }

  public boolean isSortOnTimeForLimit(boolean defaultValue)
  {
    return getContextBoolean(SORT_ON_TIME, defaultValue);
  }

  public Sequence<Row> applyLimit(Sequence<Row> sequence, boolean sortOnTimeForLimit)
  {
    if (havingSpec != null) {
      Predicate<Row> predicate = havingSpec.toEvaluator(Schema.EMPTY.resolve(this, true));
      sequence = Sequences.filter(sequence, predicate);
    }
    Query.AggregationsSupport<?> query = withPostAggregatorSpecs(
        PostAggregators.decorate(getPostAggregatorSpecs(), getAggregatorSpecs())
    );
    return limitSpec.build(query, sortOnTimeForLimit).apply(sequence);
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    List<String> outputColumns = getOutputColumns();
    if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
      return outputColumns;
    }
    if (GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs()) && lateralView == null) {
      outputColumns = Lists.newArrayList(Row.TIME_COLUMN_NAME);
      outputColumns.addAll(DimensionSpecs.toOutputNames(getDimensions()));
      for (String aggregator : AggregatorFactory.toNames(getAggregatorSpecs())) {
        if (!outputColumns.contains(aggregator)) {
          outputColumns.add(aggregator);
        }
      }
      for (String postAggregator : PostAggregators.toNames(getPostAggregatorSpecs())) {
        if (!outputColumns.contains(postAggregator)) {
          outputColumns.add(postAggregator);
        }
      }
      return outputColumns;
    }
    return null;
  }

  @Override
  public Sequence<Object[]> array(Sequence<Row> sequence)
  {
    final String[] columns = Preconditions.checkNotNull(estimatedOutputColumns()).toArray(new String[0]);
    return io.druid.common.utils.Sequences.map(
        sequence,
        new Function<Row, Object[]>()
        {
          @Override
          public Object[] apply(Row input)
          {
            final Object[] array = new Object[columns.length];
            for (int i = 0; i < columns.length; i++) {
              array[i] = Row.TIME_COLUMN_NAME.equals(columns[i]) ?
                         input.getTimestampFromEpoch() : input.getRaw(columns[i]);
            }
            return array;
          }
        }
    );
  }

  public String getLocalSplitStrategy()
  {
    if (GuavaUtils.isNullOrEmpty(limitSpec.getColumns()) && GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs())) {
      return "slopedSpaced";
    }
    return "evenSpaced";
  }

  public List<OrderByColumnSpec> getLimitOrdering(OrderedLimitSpec limiting)
  {
    List<OrderByColumnSpec> ordering = limiting.getColumns();
    if (GuavaUtils.isNullOrEmpty(ordering)) {
      ordering = getLimitSpec().getColumns();
    }
    if (GuavaUtils.isNullOrEmpty(ordering)) {
      ordering = DimensionSpecs.asOrderByColumnSpec(getDimensions());
    }
    return ordering;
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

    BaseAggregationQuery that = (BaseAggregationQuery) o;

    if (!Objects.equals(aggregatorSpecs, that.aggregatorSpecs)) {
      return false;
    }
    if (!Objects.equals(filter, that.filter)) {
      return false;
    }
    if (!Objects.equals(getDimensions(), that.getDimensions())) {
      return false;
    }
    if (!Objects.equals(virtualColumns, that.virtualColumns)) {
      return false;
    }
    if (!Objects.equals(granularity, that.granularity)) {
      return false;
    }
    if (!Objects.equals(havingSpec, that.havingSpec)) {
      return false;
    }
    if (!Objects.equals(lateralView, that.lateralView)) {
      return false;
    }
    if (!Objects.equals(limitSpec, that.limitSpec)) {
      return false;
    }
    if (!Objects.equals(postAggregatorSpecs, that.postAggregatorSpecs)) {
      return false;
    }
    if (!Objects.equals(outputColumns, that.outputColumns)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (limitSpec != null ? limitSpec.hashCode() : 0);
    result = 31 * result + (havingSpec != null ? havingSpec.hashCode() : 0);
    result = 31 * result + (lateralView != null ? lateralView.hashCode() : 0);
    result = 31 * result + (filter != null ? filter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + Objects.hashCode(getDimensions());
    result = 31 * result + (virtualColumns != null ? virtualColumns.hashCode() : 0);
    result = 31 * result + (aggregatorSpecs != null ? aggregatorSpecs.hashCode() : 0);
    result = 31 * result + (postAggregatorSpecs != null ? postAggregatorSpecs.hashCode() : 0);
    result = 31 * result + (limitSpec != null ? limitSpec.hashCode() : 0);
    result = 31 * result + (outputColumns != null ? outputColumns.hashCode() : 0);
    return result;
  }

  public static abstract class Builder<T>
  {
    protected DataSource dataSource;
    protected QuerySegmentSpec querySegmentSpec;
    protected boolean descending;
    protected DimFilter dimFilter;
    protected Granularity granularity = Granularities.ALL;
    protected List<DimensionSpec> dimensions;
    protected List<VirtualColumn> virtualColumns;
    protected List<AggregatorFactory> aggregatorSpecs;
    protected List<PostAggregator> postAggregatorSpecs;
    protected List<String> outputColumns;
    protected HavingSpec havingSpec;
    protected LateralViewSpec lateralViewSpec;

    protected Map<String, Object> context;

    protected LimitSpec limitSpec = null;
    protected List<OrderByColumnSpec> orderByColumnSpecs = Lists.newArrayList();
    protected int limit = -1;

    public Builder()
    {
    }

    public Builder(BaseAggregationQuery query)
    {
      dataSource = query.getDataSource();
      querySegmentSpec = query.getQuerySegmentSpec();
      descending = query.isDescending();
      dimFilter = query.getFilter();
      granularity = query.getGranularity();
      dimensions = query.getDimensions();
      virtualColumns = query.getVirtualColumns();
      aggregatorSpecs = query.getAggregatorSpecs();
      postAggregatorSpecs = query.getPostAggregatorSpecs();
      limitSpec = query.getLimitSpec();
      havingSpec = query.getHavingSpec();
      lateralViewSpec = query.getLateralView();
      outputColumns = query.getOutputColumns();
      context = query.getContext();
    }

    public Builder(Builder<?> builder)
    {
      dataSource = builder.dataSource;
      querySegmentSpec = builder.querySegmentSpec;
      descending = builder.descending;
      dimFilter = builder.dimFilter;
      granularity = builder.granularity;
      dimensions = builder.dimensions;
      virtualColumns = builder.virtualColumns;
      aggregatorSpecs = builder.aggregatorSpecs;
      postAggregatorSpecs = builder.postAggregatorSpecs;
      limitSpec = builder.limitSpec;
      orderByColumnSpecs = builder.orderByColumnSpecs;
      limit = builder.limit;
      havingSpec = builder.havingSpec;
      lateralViewSpec= builder.lateralViewSpec;
      outputColumns = builder.outputColumns;
      context = builder.context;
    }

    protected LimitSpec buildLimitSpec()
    {
      final LimitSpec theLimitSpec;
      if (limitSpec == null) {
        theLimitSpec = LimitSpec.of(limit, orderByColumnSpecs);
      } else {
        theLimitSpec = limitSpec;
      }
      return theLimitSpec;
    }

    public Builder<T> setDataSource(DataSource dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Builder<T> dataSource(DataSource dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Builder<T> setDataSource(String dataSource)
    {
      this.dataSource = new TableDataSource(dataSource);
      return this;
    }

    public Builder<T> dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public Builder<T> setDataSource(Query query)
    {
      this.dataSource = QueryDataSource.of(query);
      return this;
    }

    public Builder<T> dataSource(Query query)
    {
      dataSource = QueryDataSource.of(query);
      return this;
    }

    public Builder<T> setInterval(QuerySegmentSpec interval)
    {
      return setQuerySegmentSpec(interval);
    }

    public Builder<T> intervals(QuerySegmentSpec interval)
    {
      return setQuerySegmentSpec(interval);
    }

    public Builder<T> setInterval(List<Interval> intervals)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(intervals));
    }

    public Builder<T> intervals(List<Interval> intervals)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(intervals));
    }

    public Builder<T> setInterval(Interval interval)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(interval));
    }

    public Builder<T> intervals(Interval interval)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(interval));
    }

    public Builder<T> setInterval(String interval)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(interval));
    }

    public Builder<T> intervals(String interval)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(interval));
    }

    public Builder<T> setDescending(boolean descending)
    {
      this.descending = descending;
      return this;
    }

    public Builder<T> descending(boolean descending)
    {
      return setDescending(descending);
    }

    public Builder<T> limit(int limit)
    {
      ensureExplicitLimitNotSet();
      this.limit = limit;
      return this;
    }

    public Builder<T> addOrderByColumn(String dimension)
    {
      return addOrderByColumn(dimension, (Direction) null);
    }

    public Builder<T> addOrderByColumn(String dimension, String direction)
    {
      return addOrderByColumn(dimension, Direction.fromString(direction));
    }

    public Builder<T> addOrderByColumn(String dimension, Direction direction)
    {
      return addOrderByColumn(new OrderByColumnSpec(dimension, direction));
    }

    public Builder<T> addOrderByColumn(OrderByColumnSpec columnSpec)
    {
      ensureExplicitLimitNotSet();
      this.orderByColumnSpecs.add(columnSpec);
      return this;
    }

    public Builder<T> setLimitSpec(LimitSpec limitSpec)
    {
      this.limitSpec = limitSpec;
      return this;
    }

    public Builder<T> limitSpec(LimitSpec limitSpec)
    {
      return setLimitSpec(limitSpec);
    }

    public Builder<T> setOutputColumns(List<String> outputColumns)
    {
      this.outputColumns = outputColumns;
      return this;
    }

    public Builder<T> outputColumns(List<String> outputColumns)
    {
      return setOutputColumns(outputColumns);
    }

    private void ensureExplicitLimitNotSet()
    {
      if (limitSpec != null) {
        throw new ISE("Ambiguous build, limitSpec[%s] already set", limitSpec);
      }
    }

    public Builder<T> setQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
    {
      this.querySegmentSpec = querySegmentSpec;
      return this;
    }

    public Builder<T> setDimFilter(DimFilter dimFilter)
    {
      this.dimFilter = dimFilter;
      return this;
    }

    public Builder<T> filters(DimFilter dimFilter)
    {
      return setDimFilter(dimFilter);
    }

    public Builder<T> filters(String dimensionName, String value)
    {
      return setDimFilter(new SelectorDimFilter(dimensionName, value, null));
    }

    public Builder<T> filters(String dimensionName, String value, String... values)
    {
      return setDimFilter(new InDimFilter(dimensionName, Lists.asList(value, values), null));
    }

    public Builder<T> setGranularity(Granularity granularity)
    {
      this.granularity = granularity;
      return this;
    }

    public Builder<T> granularity(Granularity granularity)
    {
      return setGranularity(granularity);
    }

    public Builder<T> granularity(String granularity)
    {
      return setGranularity(Granularity.fromString(granularity));
    }

    public Builder<T> addDimension(String column)
    {
      return addDimension(column, column);
    }

    public Builder<T> addDimension(String column, String outputName)
    {
      return addDimension(new DefaultDimensionSpec(column, outputName));
    }

    public Builder<T> addDimension(DimensionSpec dimension)
    {
      if (dimensions == null) {
        dimensions = Lists.newArrayList();
      }
      dimensions.add(dimension);
      return this;
    }

    public Builder<T> setDimensions(List<DimensionSpec> dimensions)
    {
      this.dimensions = Lists.newArrayList(dimensions);
      return this;
    }

    public Builder<T> setDimensions(DimensionSpec... dimensions)
    {
      this.dimensions = Lists.newArrayList(dimensions);
      return this;
    }

    public Builder<T> dimensions(DimensionSpec... dimensions)
    {
      return setDimensions(dimensions);
    }

    public Builder<T> dimensions(List<DimensionSpec> dimensions)
    {
      return setDimensions(dimensions);
    }

    public Builder<T> setVirtualColumns(List<VirtualColumn> virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public Builder<T> virtualColumns(List<VirtualColumn> virtualColumns)
    {
      return setVirtualColumns(virtualColumns);
    }

    public Builder<T> setVirtualColumns(VirtualColumn... virtualColumns)
    {
      this.virtualColumns = Arrays.asList(virtualColumns);
      return this;
    }

    public Builder<T> virtualColumns(VirtualColumn... virtualColumns)
    {
      return setVirtualColumns(virtualColumns);
    }

    public Builder<T> addAggregator(AggregatorFactory aggregator)
    {
      if (aggregatorSpecs == null) {
        aggregatorSpecs = Lists.newArrayList();
      }

      aggregatorSpecs.add(aggregator);
      return this;
    }

    public Builder<T> setAggregatorSpecs(List<AggregatorFactory> aggregatorSpecs)
    {
      this.aggregatorSpecs = Lists.newArrayList(aggregatorSpecs);
      return this;
    }

    public Builder<T> setAggregatorSpecs(AggregatorFactory... aggregatorSpecs)
    {
      return setAggregatorSpecs(Arrays.asList(aggregatorSpecs));
    }

    public Builder<T> aggregators(List<AggregatorFactory> aggregatorSpecs)
    {
      return setAggregatorSpecs(aggregatorSpecs);
    }

    public Builder<T> aggregators(AggregatorFactory... aggregatorSpecs)
    {
      return setAggregatorSpecs(aggregatorSpecs);
    }

    public Builder<T> addPostAggregator(PostAggregator postAgg)
    {
      if (postAggregatorSpecs == null) {
        postAggregatorSpecs = Lists.newArrayList();
      }
      postAggregatorSpecs.add(postAgg);
      return this;
    }

    public Builder<T> setPostAggregatorSpecs(List<PostAggregator> postAggregatorSpecs)
    {
      this.postAggregatorSpecs = Lists.newArrayList(postAggregatorSpecs);
      return this;
    }

    public Builder<T> setPostAggregatorSpecs(PostAggregator... postAggregatorSpecs)
    {
      return setPostAggregatorSpecs(Arrays.asList(postAggregatorSpecs));
    }

    public Builder<T> postAggregators(List<PostAggregator> postAggregatorSpecs)
    {
      return setPostAggregatorSpecs(postAggregatorSpecs);
    }

    public Builder<T> postAggregators(PostAggregator... postAggregatorSpecs)
    {
      return setPostAggregatorSpecs(postAggregatorSpecs);
    }

    public Builder<T> setContext(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public Builder<T> context(Map<String, Object> context)
    {
      return setContext(context);
    }

    public Builder<T> overrideContext(Map<String, Object> override)
    {
      return setContext(BaseQuery.overrideContextWith(context, override));
    }

    public Builder<T> addContext(String key, Object value)
    {
      if (context == null) {
        context = Maps.newHashMap();
      } else {
        context = Maps.newHashMap(context);
      }
      context.put(key, value);
      return this;
    }

    public Builder<T> setHavingSpec(HavingSpec havingSpec)
    {
      this.havingSpec = havingSpec;
      return this;
    }

    public Builder<T> havingSpec(HavingSpec havingSpec)
    {
      return setHavingSpec(havingSpec);
    }

    public Builder<T> setLateralViewSpec(LateralViewSpec lateralViewSpec)
    {
      this.lateralViewSpec = lateralViewSpec;
      return this;
    }

    public Builder<T> lateralViewSpec(LateralViewSpec lateralViewSpec)
    {
      return setLateralViewSpec(lateralViewSpec);
    }

    public Builder<T> setLimit(Integer limit)
    {
      this.limit = limit;
      return this;
    }

    public abstract T build();
  }
}
