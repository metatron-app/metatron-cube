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

package io.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.LateralViewSpec;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.NoopLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
public class GroupByQuery extends BaseQuery<Row> implements Query.DimensionSupport<Row>
{
  public static final String SORT_ON_TIME = "groupby.sort.on.time";

  public static Builder builder()
  {
    return new Builder();
  }

  private final LimitSpec limitSpec;
  private final HavingSpec havingSpec;
  private final LateralViewSpec lateralView;
  private final DimFilter dimFilter;
  private final Granularity granularity;
  private final List<DimensionSpec> dimensions;
  private final List<VirtualColumn> virtualColumns;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;
  private final List<String> outputColumns;

  @JsonCreator
  public GroupByQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
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
    super(dataSource, querySegmentSpec, false, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    for (DimensionSpec spec : this.dimensions) {
      Preconditions.checkArgument(spec != null, "dimensions has null DimensionSpec");
    }
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.aggregatorSpecs = aggregatorSpecs == null ? ImmutableList.<AggregatorFactory>of() : aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;
    this.havingSpec = havingSpec;
    this.lateralView = lateralView;
    this.limitSpec = (limitSpec == null) ? new NoopLimitSpec() : limitSpec;
    this.outputColumns = outputColumns;

    Preconditions.checkNotNull(this.granularity, "Must specify a granularity");
    Preconditions.checkNotNull(this.aggregatorSpecs, "Must specify at least one aggregator");
    Queries.verifyAggregations(this.aggregatorSpecs, this.postAggregatorSpecs);
  }

  @Override
  @JsonProperty("filter")
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
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty("aggregations")
  public List<AggregatorFactory> getAggregatorSpecs()
  {
    return aggregatorSpecs;
  }

  @JsonProperty("postAggregations")
  public List<PostAggregator> getPostAggregatorSpecs()
  {
    return postAggregatorSpecs;
  }

  @JsonProperty("having")
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
  public List<String> getOutputColumns()
  {
    return outputColumns;
  }

  @JsonProperty
  public LateralViewSpec getLateralView()
  {
    return lateralView;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null || super.hasFilters();
  }

  @Override
  public String getType()
  {
    return GROUP_BY;
  }

  public Sequence<Row> applyLimit(Sequence<Row> results, GroupByQueryConfig config)
  {
    boolean sortOnTimeForLimit = getContextBoolean(SORT_ON_TIME, config.isSortOnTime());

    Function<Sequence<Row>, Sequence<Row>> postProcFn =
        limitSpec.build(dimensions, aggregatorSpecs, postAggregatorSpecs, sortOnTimeForLimit);

    if (havingSpec != null) {
      postProcFn = Functions.compose(
          postProcFn,
          new Function<Sequence<Row>, Sequence<Row>>()
          {
            @Override
            public Sequence<Row> apply(Sequence<Row> input)
            {
              return Sequences.filter(
                  input,
                  new Predicate<Row>()
                  {
                    @Override
                    public boolean apply(Row input)
                    {
                      return havingSpec.eval(input);
                    }
                  }
              );
            }
          }
      );
    }

    return postProcFn.apply(results);
  }

  @Override
  public boolean allDimensionsForEmpty()
  {
    return BaseQuery.allDimensionsForEmpty(this, false);
  }

  @Override
  public boolean neededForDimension(String column)
  {
    for (DimensionSpec dimensionSpec : dimensions) {
      if (column.equals(dimensionSpec.getOutputName())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public GroupByQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        dimensions,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public GroupByQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new GroupByQuery(
        getDataSource(),
        spec,
        dimFilter,
        granularity,
        dimensions,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public GroupByQuery withDimFilter(final DimFilter dimFilter)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimFilter,
        getGranularity(),
        getDimensions(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        getLimitSpec(),
        getOutputColumns(),
        getLateralView(),
        getContext()
    );
  }

  @Override
  public Query<Row> withDataSource(DataSource dataSource)
  {
    return new GroupByQuery(
        dataSource,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        dimensions,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        getContext()
    );
  }

  @Override
  public GroupByQuery withDimensionSpecs(final List<DimensionSpec> dimensionSpecs)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        dimensionSpecs,
        getVirtualColumns(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        getLimitSpec(),
        getOutputColumns(),
        getLateralView(),
        getContext()
    );
  }

  @Override
  public GroupByQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        virtualColumns,
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        getLimitSpec(),
        getOutputColumns(),
        getLateralView(),
        getContext()
    );
  }

  public GroupByQuery withLimitSpec(final LimitSpec limitSpec)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        limitSpec,
        getOutputColumns(),
        getLateralView(),
        getContext()
    );
  }

  public GroupByQuery withOutputColumns(final List<String> outputColumns)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        getLimitSpec(),
        outputColumns,
        getLateralView(),
        getContext()
    );
  }

  public GroupByQuery withAggregatorSpecs(final List<AggregatorFactory> aggregatorSpecs)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getVirtualColumns(),
        aggregatorSpecs,
        getPostAggregatorSpecs(),
        getHavingSpec(),
        getLimitSpec(),
        getOutputColumns(),
        getLateralView(),
        getContext()
    );
  }

  public GroupByQuery withHavingSpec(final HavingSpec havingSpec)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        havingSpec,
        getLimitSpec(),
        getOutputColumns(),
        getLateralView(),
        getContext()
    );
  }

  public GroupByQuery withPostAggregatorSpecs(final List<PostAggregator> postAggregatorSpecs)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        postAggregatorSpecs,
        getHavingSpec(),
        getLimitSpec(),
        getOutputColumns(),
        getLateralView(),
        getContext()
    );
  }

  public static class Builder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private DimFilter dimFilter;
    private Granularity granularity;
    private List<DimensionSpec> dimensions;
    private List<VirtualColumn> virtualColumns;
    private List<AggregatorFactory> aggregatorSpecs;
    private List<PostAggregator> postAggregatorSpecs;
    private List<String> outputColumns;
    private HavingSpec havingSpec;
    private LateralViewSpec lateralViewSpec;

    private Map<String, Object> context;

    private LimitSpec limitSpec = null;
    private List<OrderByColumnSpec> orderByColumnSpecs = Lists.newArrayList();
    private int limit = Integer.MAX_VALUE;

    public Builder()
    {
    }

    public Builder(GroupByQuery query)
    {
      dataSource = query.getDataSource();
      querySegmentSpec = query.getQuerySegmentSpec();
      limitSpec = query.getLimitSpec();
      dimFilter = query.getDimFilter();
      granularity = query.getGranularity();
      dimensions = query.getDimensions();
      virtualColumns = query.getVirtualColumns();
      aggregatorSpecs = query.getAggregatorSpecs();
      postAggregatorSpecs = query.getPostAggregatorSpecs();
      havingSpec = query.getHavingSpec();
      outputColumns = query.getOutputColumns();
      context = query.getContext();
    }

    public Builder(Builder builder)
    {
      dataSource = builder.dataSource;
      querySegmentSpec = builder.querySegmentSpec;
      limitSpec = builder.limitSpec;
      dimFilter = builder.dimFilter;
      granularity = builder.granularity;
      dimensions = builder.dimensions;
      virtualColumns = builder.virtualColumns;
      aggregatorSpecs = builder.aggregatorSpecs;
      postAggregatorSpecs = builder.postAggregatorSpecs;
      havingSpec = builder.havingSpec;
      limit = builder.limit;
      outputColumns = builder.outputColumns;

      context = builder.context;
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
      this.dataSource = new QueryDataSource(query);
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

    public Builder limit(int limit)
    {
      ensureExplicitLimitNotSet();
      this.limit = limit;
      return this;
    }

    public Builder addOrderByColumn(String dimension)
    {
      return addOrderByColumn(dimension, (OrderByColumnSpec.Direction) null);
    }

    public Builder addOrderByColumn(String dimension, String direction)
    {
      return addOrderByColumn(dimension, OrderByColumnSpec.determineDirection(direction));
    }

    public Builder addOrderByColumn(String dimension, OrderByColumnSpec.Direction direction)
    {
      return addOrderByColumn(new OrderByColumnSpec(dimension, direction));
    }

    public Builder addOrderByColumn(OrderByColumnSpec columnSpec)
    {
      ensureExplicitLimitNotSet();
      this.orderByColumnSpecs.add(columnSpec);
      return this;
    }

    public Builder setLimitSpec(LimitSpec limitSpec)
    {
      ensureFluentLimitsNotSet();
      this.limitSpec = limitSpec;
      return this;
    }

    public Builder setOutputColumns(List<String> outputColumns)
    {
      this.outputColumns = outputColumns;
      return this;
    }

    private void ensureExplicitLimitNotSet()
    {
      if (limitSpec != null) {
        throw new ISE("Ambiguous build, limitSpec[%s] already set", limitSpec);
      }
    }

    private void ensureFluentLimitsNotSet()
    {
      if (!(limit == Integer.MAX_VALUE && orderByColumnSpecs.isEmpty())) {
        throw new ISE("Ambiguous build, limit[%s] or columnSpecs[%s] already set.", limit, orderByColumnSpecs);
      }
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

    public Builder setVirtualColumns(List<VirtualColumn> virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public Builder addAggregator(AggregatorFactory aggregator)
    {
      if (aggregatorSpecs == null) {
        aggregatorSpecs = Lists.newArrayList();
      }

      aggregatorSpecs.add(aggregator);
      return this;
    }

    public Builder setAggregatorSpecs(List<AggregatorFactory> aggregatorSpecs)
    {
      this.aggregatorSpecs = Lists.newArrayList(aggregatorSpecs);
      return this;
    }

    public Builder addPostAggregator(PostAggregator postAgg)
    {
      if (postAggregatorSpecs == null) {
        postAggregatorSpecs = Lists.newArrayList();
      }

      postAggregatorSpecs.add(postAgg);
      return this;
    }

    public Builder setPostAggregatorSpecs(List<PostAggregator> postAggregatorSpecs)
    {
      this.postAggregatorSpecs = Lists.newArrayList(postAggregatorSpecs);
      return this;
    }

    public Builder setContext(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public Builder setHavingSpec(HavingSpec havingSpec)
    {
      this.havingSpec = havingSpec;
      return this;
    }

    public Builder setLateralViewSpec(LateralViewSpec lateralViewSpec)
    {
      this.lateralViewSpec = lateralViewSpec;
      return this;
    }

    public Builder setLimit(Integer limit)
    {
      this.limit = limit;

      return this;
    }

    public Builder copy()
    {
      return new Builder(this);
    }

    public GroupByQuery build()
    {
      final LimitSpec theLimitSpec;
      if (limitSpec == null) {
        if (orderByColumnSpecs.isEmpty() && limit == Integer.MAX_VALUE) {
          theLimitSpec = new NoopLimitSpec();
        } else {
          theLimitSpec = new DefaultLimitSpec(orderByColumnSpecs, limit);
        }
      } else {
        theLimitSpec = limitSpec;
      }

      return new GroupByQuery(
          dataSource,
          querySegmentSpec,
          dimFilter,
          granularity,
          dimensions,
          virtualColumns,
          aggregatorSpecs,
          postAggregatorSpecs,
          havingSpec,
          theLimitSpec,
          outputColumns,
          lateralViewSpec,
          context
      );
    }
  }

  @Override
  public String toString()
  {
    return "GroupByQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", limitSpec=" + limitSpec +
           ", dimFilter=" + dimFilter +
           ", granularity=" + granularity +
           ", dimensions=" + dimensions +
           ", virtualColumns=" + virtualColumns +
           ", aggregatorSpecs=" + aggregatorSpecs +
           ", postAggregatorSpecs=" + postAggregatorSpecs +
           ", havingSpec=" + havingSpec +
           ", outputColumns=" + outputColumns +
           ", explodeSpec=" + lateralView +
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

    GroupByQuery that = (GroupByQuery) o;

    if (aggregatorSpecs != null ? !aggregatorSpecs.equals(that.aggregatorSpecs) : that.aggregatorSpecs != null) {
      return false;
    }
    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) {
      return false;
    }
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) {
      return false;
    }
    if (virtualColumns != null ? !virtualColumns.equals(that.virtualColumns) : that.virtualColumns != null) {
      return false;
    }
    if (granularity != null ? !granularity.equals(that.granularity) : that.granularity != null) {
      return false;
    }
    if (havingSpec != null ? !havingSpec.equals(that.havingSpec) : that.havingSpec != null) {
      return false;
    }
    if (lateralView != null ? !lateralView.equals(that.lateralView) : that.lateralView != null) {
      return false;
    }
    if (limitSpec != null ? !limitSpec.equals(that.limitSpec) : that.limitSpec != null) {
      return false;
    }
    if (postAggregatorSpecs != null
        ? !postAggregatorSpecs.equals(that.postAggregatorSpecs)
        : that.postAggregatorSpecs != null) {
      return false;
    }
    if (outputColumns != null ? !outputColumns.equals(that.outputColumns) : that.outputColumns != null) {
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
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (virtualColumns != null ? virtualColumns.hashCode() : 0);
    result = 31 * result + (aggregatorSpecs != null ? aggregatorSpecs.hashCode() : 0);
    result = 31 * result + (postAggregatorSpecs != null ? postAggregatorSpecs.hashCode() : 0);
    result = 31 * result + (limitSpec != null ? limitSpec.hashCode() : 0);
    result = 31 * result + (outputColumns != null ? outputColumns.hashCode() : 0);
    return result;
  }

  @Override
  public Ordering getResultOrdering()
  {
    final Comparator naturalNullsFirst = Ordering.natural().nullsFirst();
    final Ordering<Row> rowOrdering = getRowOrdering(false);

    return Ordering.from(
        new Comparator<Object>()
        {
          @Override
          public int compare(Object lhs, Object rhs)
          {
            if (lhs instanceof Row) {
              return rowOrdering.compare((Row) lhs, (Row) rhs);
            } else {
              // Probably bySegment queries
              return naturalNullsFirst.compare(lhs, rhs);
            }
          }
        }
    );
  }

  public Ordering<Row> getRowOrdering(final boolean granular)
  {
    final Comparator<Object> naturalNullsFirst = GuavaUtils.nullFirstNatural();
    final String[] outputNames = new String[dimensions.size()];
    for (int i = 0; i < outputNames.length; i++) {
      outputNames[i] = dimensions.get(i).getOutputName();
    }
    return Ordering.from(
        new Comparator<Row>()
        {
          @Override
          public int compare(Row lhs, Row rhs)
          {
            final int timeCompare;

            if (granular) {
              timeCompare = Longs.compare(
                  granularity.bucketStart(lhs.getTimestamp()).getMillis(),
                  granularity.bucketStart(rhs.getTimestamp()).getMillis()
              );
            } else {
              timeCompare = Longs.compare(
                  lhs.getTimestampFromEpoch(),
                  rhs.getTimestampFromEpoch()
              );
            }

            if (timeCompare != 0) {
              return timeCompare;
            }

            for (String outputName : outputNames) {
              final int dimCompare = naturalNullsFirst.compare(
                  lhs.getRaw(outputName),
                  rhs.getRaw(outputName)
              );
              if (dimCompare != 0) {
                return dimCompare;
              }
            }

            return 0;
          }
        }
    );
  }
}
