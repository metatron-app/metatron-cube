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

package io.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.Accumulator;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.Pair;
import io.druid.query.ArrayToRow;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.JoinQuery;
import io.druid.query.LateralViewSpec;
import io.druid.query.PostProcessingOperator;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.Result;
import io.druid.query.SequenceCountingProcessor;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.InDimFilter;
import io.druid.query.frequency.FrequencyQuery;
import io.druid.query.groupby.having.AlwaysHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.ordering.Direction;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNMetricSpec;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.VirtualColumn;
import org.apache.commons.lang.mutable.MutableInt;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class GroupByQuery extends BaseAggregationQuery implements Query.RewritingQuery<Row>
{
  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(BaseAggregationQuery aggregationQuery)
  {
    return new Builder(aggregationQuery);
  }

  private final List<DimensionSpec> dimensions;
  private final GroupingSetSpec groupingSets;

  @JsonCreator
  public GroupByQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("groupingSets") GroupingSetSpec groupingSets,
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
    super(
        dataSource,
        querySegmentSpec,
        false,
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        context
    );
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.groupingSets = groupingSets == null || groupingSets.isEmpty() ? null : groupingSets;
    for (DimensionSpec spec : this.dimensions) {
      Preconditions.checkArgument(spec != null, "dimensions has null DimensionSpec");
    }
    Queries.verifyAggregations(
        DimensionSpecs.toOutputNames(getDimensions()), getAggregatorSpecs(), getPostAggregatorSpecs()
    );
    if (groupingSets != null) {
      groupingSets.validate(DimensionSpecs.toOutputNames(getDimensions()));
    }
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public GroupingSetSpec getGroupingSets()
  {
    return groupingSets;
  }

  @Override
  public String getType()
  {
    return GROUP_BY;
  }

  @Override
  public GroupByQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        granularity,
        dimensions,
        groupingSets,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public GroupByQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new GroupByQuery(
        getDataSource(),
        spec,
        filter,
        granularity,
        dimensions,
        groupingSets,
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
  public GroupByQuery withFilter(final DimFilter filter)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
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
  public GroupByQuery withDataSource(DataSource dataSource)
  {
    return new GroupByQuery(
        dataSource,
        getQuerySegmentSpec(),
        filter,
        granularity,
        dimensions,
        groupingSets,
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
        getFilter(),
        getGranularity(),
        dimensionSpecs,
        getGroupingSets(),
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
        getFilter(),
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
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

  @Override
  public GroupByQuery withGranularity(Granularity granularity)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        granularity,
        getDimensions(),
        getGroupingSets(),
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

  public GroupByQuery withGroupingSet(GroupingSetSpec groupingSet)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        groupingSet,
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
  public GroupByQuery withLimitSpec(final LimitSpec limitSpec)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        limitSpec == null || limitSpec.isNoop() ? null : limitSpec,
        getOutputColumns(),
        getLateralView(),
        getContext()
    );
  }

  @Override
  public GroupByQuery withOutputColumns(final List<String> outputColumns)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
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

  @Override
  public GroupByQuery withLateralView(LateralViewSpec lateralView)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        getLimitSpec(),
        getOutputColumns(),
        lateralView,
        getContext()
    );
  }

  @Override
  public GroupByQuery withAggregatorSpecs(final List<AggregatorFactory> aggregatorSpecs)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
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

  @Override
  public GroupByQuery withPostAggregatorSpecs(final List<PostAggregator> postAggregatorSpecs)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
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

  @Override
  public GroupByQuery withHavingSpec(final HavingSpec havingSpec)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
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

  @Override
  public GroupByQuery withOverriddenContext(String contextKey, Object contextValue)
  {
    return (GroupByQuery) super.withOverriddenContext(contextKey, contextValue);
  }

  @Override
  public GroupByQuery toLocalQuery()
  {
    Map<String, Object> context = computeOverriddenContext(DEFAULT_DATALOCAL_CONTEXT);
    context.put(LOCAL_SPLIT_STRATEGY, getLocalSplitStrategy());

    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        null,
        null,
        getLimitSpec().withNoLocalProcessing(),
        null,
        null,
        context
    );
  }

  @Override
  public Query rewriteQuery(QuerySegmentWalker segmentWalker)
  {
    GroupByQuery query = this;
    if (query.getContextValue(Query.LOCAL_POST_PROCESSING) != null) {
      return query;
    }
    GroupByQueryConfig groupByConfig = segmentWalker.getGroupByConfig();
    if (query.getContextBoolean(GBY_PRE_ORDERING, groupByConfig.isPreOrdering())) {
      query = query.tryPreOrdering();
    }
    if (query.getContextBoolean(GBY_REMOVE_ORDERING, groupByConfig.isRemoveOrdering())) {
      query = query.tryRemoveOrdering();
    }
    if (query.getContextBoolean(GBY_CONVERT_TIMESERIES, groupByConfig.isConvertTimeseries())) {
      Query converted = query.tryConvertToTimeseries(segmentWalker.getMapper());
      if (converted != null) {
        return converted;
      }
    }
    if (query.getContextBoolean(GBY_CONVERT_FREQUENCY, groupByConfig.isConvertFrequency())) {
      Query converted = query.tryConvertToFrequency(segmentWalker);
      if (converted != null) {
        return converted;
      }
    }
    int estimationFactor = query.getContextInt(GBY_ESTIMATE_TOPN_FACTOR, groupByConfig.getEstimateTopNFactor());
    if (estimationFactor > 0) {
      return query.tryEstimateTopN(segmentWalker, estimationFactor);
    }
    return query;
  }

  private GroupByQuery tryPreOrdering()
  {
    GroupByQuery query = this;
    if (!(query.getDataSource() instanceof TableDataSource)) {
      return query;
    }
    List<DimensionSpec> dimensionSpecs = query.getDimensions();
    if (dimensionSpecs.isEmpty()) {
      return query;
    }
    LimitSpec limitSpec = query.getLimitSpec();
    if (!GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs())) {
      List<WindowingSpec> windowingSpecs = Lists.newArrayList(limitSpec.getWindowingSpecs());
      WindowingSpec first = windowingSpecs.get(0);
      List<DimensionSpec> rewritten = applyExplicitOrdering(first.getRequiredOrdering(), dimensionSpecs);
      if (rewritten != null) {
        windowingSpecs.set(0, first.skipSorting());
        query = query.withLimitSpec(limitSpec.withWindowing(windowingSpecs))
                     .withDimensionSpecs(rewritten);
      }
    } else if (!GuavaUtils.isNullOrEmpty(limitSpec.getColumns())) {
      List<DimensionSpec> rewritten = applyExplicitOrdering(limitSpec.getColumns(), dimensionSpecs);
      if (rewritten != null) {
        query = query.withLimitSpec(limitSpec.withOrderingSpec(null))
                     .withDimensionSpecs(rewritten);
      }
    }
    return query;
  }

  private List<DimensionSpec> applyExplicitOrdering(
      List<OrderByColumnSpec> orderByColumns,
      List<DimensionSpec> dimensionSpecs
  )
  {
    if (orderByColumns.size() > dimensionSpecs.size()) {
      // todo discompose order-by if possible
      return null;
    }
    List<DimensionSpec> orderedDimensionSpecs = Lists.newArrayList();
    List<String> dimensionNames = DimensionSpecs.toOutputNames(dimensionSpecs);
    for (OrderByColumnSpec orderBy : orderByColumns) {
      int index = dimensionNames.indexOf(orderBy.getDimension());
      if (index < 0) {
        return null;
      }
      dimensionNames.set(index, null);
      DimensionSpec dimensionSpec = dimensionSpecs.get(index);
      if (dimensionSpec instanceof DimensionSpecWithOrdering) {
        DimensionSpecWithOrdering explicit = (DimensionSpecWithOrdering) dimensionSpec;
        if (!orderBy.isSameOrdering(explicit.getDirection(), explicit.getOrdering())) {
          return null;  // order conflict
        }
      }
      if (!orderBy.isBasicOrdering()) {
        dimensionSpec = new DimensionSpecWithOrdering(
            dimensionSpec, orderBy.getDirection(), orderBy.getDimensionOrder()
        );
      }
      orderedDimensionSpecs.add(dimensionSpec);
    }
    // add remaining dimensions
    for (int i = 0; i < dimensionNames.size(); i++) {
      if (dimensionNames.get(i) != null) {
        orderedDimensionSpecs.add(dimensionSpecs.get(i));
      }
    }
    return orderedDimensionSpecs;
  }

  private GroupByQuery tryRemoveOrdering()
  {
    if (getDataSource() instanceof TableDataSource &&
        GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs()) &&
        !dimensions.isEmpty() && LimitSpecs.inGroupByOrdering(limitSpec.getColumns(), dimensions)) {
      return withLimitSpec(LimitSpecs.of(limitSpec.getLimit()));
    }
    return this;
  }

  private Query tryConvertToTimeseries(ObjectMapper jsonMapper)
  {
    return dimensions.isEmpty() ? Druids.newTimeseriesQueryBuilder().copy(this).build() : null;
  }

  private Query tryConvertToFrequency(QuerySegmentWalker segmentWalker)
  {
    if (!Granularities.isAll(granularity) || lateralView != null || havingSpec != null) {
      return null;
    }
    if (dimensions.isEmpty() || !postAggregatorSpecs.isEmpty()) {
      return null;
    }
    if (!limitSpec.getWindowingSpecs().isEmpty()) {
      return null;
    }
    if (!(Iterables.getOnlyElement(aggregatorSpecs, null) instanceof CountAggregatorFactory)) {
      return null;
    }
    final List<OrderByColumnSpec> orderings = limitSpec.getColumns();
    if (!limitSpec.hasLimit() || limitSpec.getLimit() > FrequencyQuery.MAX_LIMIT || orderings.isEmpty()) {
      return null;
    }
    final String name = aggregatorSpecs.get(0).getName();
    final OrderByColumnSpec ordering = orderings.get(0);
    if (!ordering.isNaturalOrdering() || ordering.getDirection() != Direction.DESCENDING ||
        !name.equals(ordering.getDimension())) {
      return null;
    }
    LimitSpec newLimitSpec = LimitSpec.of(
        limitSpec.getLimit(), ImmutableList.copyOf(orderings.subList(1, orderings.size()))
    );
    AggregatorFactory factory = new CardinalityAggregatorFactory(
        "$v", null, dimensions, getGroupingSets(), null, true, true, 0
    );
    TimeseriesQuery meta = new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getGranularity(),
        getVirtualColumns(),
        Arrays.asList(factory),
        null,
        null,
        null,
        Arrays.asList("$v"),
        null,
        BaseQuery.copyContextForMeta(this)
    );
    GroupByQueryConfig config = segmentWalker.getGroupByConfig();
    Number cardinality = (Number) QueryRunners.only(meta, segmentWalker).getRaw("$v");
    if (cardinality == null || cardinality.longValue() < config.getConvertFrequencyCardinality()) {
      return null;  // group by would be faster
    }
    int value = cardinality.intValue();
    BigInteger prime = BigInteger.valueOf(value).nextProbablePrime();
    int width = Math.min(prime.intValue(), (value << 1) - 1);

    FrequencyQuery query = new FrequencyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getVirtualColumns(),
        getGroupingSets(),
        dimensions,
        width,
        0,
        newLimitSpec,
        null,
        getContext()
    ).rewriteQuery(segmentWalker);

    return PostProcessingOperators.prepend(
        query,
        new ArrayToRow(GuavaUtils.concat(name, DimensionSpecs.toOutputNames(dimensions)), Row.TIME_COLUMN_NAME)
    );
  }

  private Query tryEstimateTopN(QuerySegmentWalker segmentWalker, int estimationFactor)
  {
    if (!limitSpec.hasLimit()) {
      return this;
    }
    if (getGroupingSets() != null || !Granularities.isAll(getGranularity())) {
      return this;
    }
    if (dimensions.size() != 1 || !GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs()) || lateralView != null) {
      return this;
    }
    DimensionSpec dimensionSpec = dimensions.get(0);
    if (dimensionSpec instanceof DimensionSpecWithOrdering) {
      dimensionSpec = ((DimensionSpecWithOrdering) dimensions).getDelegate();
    }
    int estimationTopN = limitSpec.getLimit() * estimationFactor;
    if (estimationTopN > 0x10000 || limitSpec.getColumns().size() != 1) {
      return this;
    }
    OrderByColumnSpec orderBy = limitSpec.getColumns().get(0);
    if (!orderBy.isNaturalOrdering()) {
      return this;
    }
    String metric = orderBy.getDimension();
    Pair<List<AggregatorFactory>, List<PostAggregator>> condensed = AggregatorUtil.condensedAggregators(
        aggregatorSpecs, postAggregatorSpecs, metric
    );
    if (condensed.lhs.isEmpty() && condensed.rhs.isEmpty()) {
      return this;
    }
    Direction direction = orderBy.getDirection() == null ? Direction.ASCENDING : orderBy.getDirection();
    TopNMetricSpec metricSpec = new NumericTopNMetricSpec(metric, direction);

    final String dimension = dimensionSpec.getOutputName();
    final Set<String> dimensionValues = Sets.newHashSet();

    QueryRunners.run(new TopNQuery(
        getDataSource(),
        getVirtualColumns(),
        dimensionSpec,
        metricSpec,
        limitSpec.getLimit() * estimationFactor,
        getQuerySegmentSpec(),
        getFilter(),
        getGranularity(),
        condensed.lhs,
        condensed.rhs,
        getOutputColumns(),
        BaseQuery.copyContextForMeta(this)
    ), segmentWalker).accumulate(
        null,
        new Accumulator<Object, Result<TopNResultValue>>()
        {
          @Override
          public Object accumulate(Object accumulated, Result<TopNResultValue> in)
          {
            for (Map<String, Object> row : in.getValue().getValue()) {
              dimensionValues.add(Objects.toString(row.get(dimension), ""));
            }
            return null;
          }
        }
    );
    DimFilter filter = InDimFilter.of(dimensionSpec.getDimension(), dimensionValues, dimensionSpec.getExtractionFn());
    // seemed not need to split now
    return withFilter(DimFilters.and(filter, getFilter())).withOverriddenContext(GBY_LOCAL_SPLIT_NUM, -1);
  }

  public boolean isStreamingAggregatable(List<Segment> segments)
  {
    return getContextValue(Query.GROUPED_DIMENSIONS) == null &&
           DimensionSpecs.isAllDefault(dimensions) && groupingSets == null &&
           Segments.isAllIndexedSingleValuedDimensions(segments, DimensionSpecs.toInputNames(dimensions));
  }

  public boolean isVectorizable(List<Segment> segments)
  {
    return Aggregators.allVectorizable(aggregatorSpecs) &&
           Segments.isVectorizableFactories(segments, aggregatorSpecs) &&
           Segments.isVectorizableDimensions(segments, DimensionSpecs.toInputNames(dimensions));
  }

  @JsonIgnore
  public int sortedIndex(List<String> columns)
  {
    return dimensions.isEmpty() || !Granularities.isAll(granularity) ? -1 : columns.indexOf(dimensions.get(0).getOutputName());
  }

  public int[][] getGroupings()
  {
    return groupingSets == null
           ? GroupingSetSpec.EMPTY_INDEX
           : groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensions));
  }

  public static class Builder extends BaseAggregationQuery.Builder<GroupByQuery>
  {
    private GroupingSetSpec groupingSets;

    public Builder() {}

    public Builder(BaseAggregationQuery aggregationQuery)
    {
      super(aggregationQuery);
    }

    public Builder setGroupingSets(GroupingSetSpec groupingSets)
    {
      this.groupingSets = groupingSets;
      return this;
    }

    public Builder groupingSets(GroupingSetSpec groupingSets)
    {
      return setGroupingSets(groupingSets);
    }

    @Override
    public GroupByQuery build()
    {
      return new GroupByQuery(
          dataSource,
          querySegmentSpec,
          dimFilter,
          granularity,
          dimensions,
          groupingSets,
          virtualColumns,
          aggregatorSpecs,
          postAggregatorSpecs,
          havingSpec,
          buildLimitSpec(),
          outputColumns,
          lateralViewSpec,
          context
      );
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), groupingSets);
  }

  @Override
  public boolean equals(Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    return Objects.equals(groupingSets, ((GroupByQuery) o).groupingSets);
  }

  @Override
  public String toString()
  {
    return "GroupByQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           (getQuerySegmentSpec() == null ? "" : ", querySegmentSpec=" + getQuerySegmentSpec()) +
           (Granularities.isAll(granularity) ? "" : ", granularity=" + granularity) +
           ", dimensions=" + dimensions +
           (filter == null ? "" : ", filter=" + filter) +
           (groupingSets == null ? "" : ", groupingSets=" + groupingSets) +
           (virtualColumns.isEmpty() ? "" : ", virtualColumns=" + virtualColumns) +
           (aggregatorSpecs.isEmpty() ? "" : ", aggregatorSpecs=" + aggregatorSpecs) +
           (postAggregatorSpecs.isEmpty() ? "" : ", postAggregatorSpecs=" + postAggregatorSpecs) +
           (havingSpec == null ? "" : ", havingSpec=" + havingSpec) +
           (limitSpec.isNoop() ? "" : ", limitSpec=" + limitSpec) +
           (outputColumns == null ? "" : ", outputColumns=" + outputColumns) +
           (lateralView == null ? "" : "lateralView=" + lateralView) +
           toString(POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT, JoinQuery.HASHING) +
           '}';
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return super.append(builder.append(0x03))
                .append(granularity).append(dimensions).append(filter).append(groupingSets)
                .append(virtualColumns).append(aggregatorSpecs).append(postAggregatorSpecs).append(havingSpec)
                .append(limitSpec).append(outputColumns).append(lateralView);
  }

  @Override
  public Comparator<Row> getMergeOrdering(List<String> columns)
  {
    return isBySegment(this) ? GuavaUtils.<Row>nullFirstNatural() : getCompactRowOrdering();
  }

  Comparator<Row> getCompactRowOrdering()
  {
    final Comparator<Object[]> comparator = DimensionSpecs.toComparator(dimensions, granularity);
    return (r1, r2) -> comparator.compare(((CompactRow) r1).getValues(), ((CompactRow) r2).getValues());
  }

  public TimeseriesQuery asTimeseriesQuery()
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        granularity,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limitSpec,
        outputColumns,
        lateralView,
        BaseQuery.copyContextForMeta(this)
    );
  }

  public TopNQuery asTopNQuery(DimensionSpec dimensionSpec, TopNMetricSpec metricSpec)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        dimensionSpec,
        metricSpec,
        limitSpec.getLimit(),
        getQuerySegmentSpec(),
        filter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        BaseQuery.copyContextForMeta(this)
    );
  }

  @SuppressWarnings("unchecked")
  public Query<Row> toCardinalityEstimator(QuerySegmentWalker walker)
  {
    Query<Row> rewritten = toCardinalityEstimator(walker.getMapper(), true);
    if (rewritten instanceof Query.RewritingQuery) {
      rewritten = ((Query.RewritingQuery) rewritten).rewriteQuery(walker);
    }
    return rewritten;
  }

  private Query<Row> toCardinalityEstimator(ObjectMapper mapper, boolean throwException)
  {
    GroupByQuery query = this;
    if (query.getLateralView() != null ||
        (query.getHavingSpec() != null && !(query.getHavingSpec() instanceof AlwaysHavingSpec))) {
      if (throwException) {
        throw new IllegalStateException("cannot estimate group-by with lateral view or having clause");
      }
      return toWorstCase(query, mapper);
    }
    List<String> outputNames = DimensionSpecs.toOutputNames(query.getDimensions());
    LimitSpec limitSpec = query.getLimitSpec();
    List<String> candidate = outputNames;
    if (limitSpec != null && !GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs())) {
      List<String> metrics = GuavaUtils.concat(
          AggregatorFactory.toNames(query.getAggregatorSpecs()),
          PostAggregators.toNames(query.getPostAggregatorSpecs())
      );
      for (WindowingSpec windowingSpec : limitSpec.getWindowingSpecs()) {
        List<String> partitionColumns = windowingSpec.getPartitionColumns();
        if (windowingSpec.getPivotSpec() != null || windowingSpec.getFlattenSpec() != null) {
          if (GuavaUtils.containsAny(partitionColumns, metrics)) {
            if (throwException) {
              throw new IllegalStateException("cannot estimate group-by partitioned by metric");
            }
            return toWorstCase(query, mapper);
          }
          List<String> retained = partitionColumns.isEmpty()
                                  ? partitionColumns
                                  : GuavaUtils.retain(candidate, partitionColumns);
          if (candidate.size() > retained.size()) {
            candidate = retained;
          }
        }
      }
    }
    GroupingSetSpec groupingSet = query.getGroupingSets();
    if (groupingSet != null && candidate.size() > 1 && candidate.size() < outputNames.size()) {
      int[] mapping = GuavaUtils.indexOf(outputNames, candidate);
      int[][] indices = groupingSet.getGroupings(outputNames);
      List<List<Integer>> list = Lists.newArrayList();
      for (int[] index : indices) {
        List<Integer> rewritten = Lists.newArrayList();
        for (int x : index) {
          if (mapping[x] >= 0) {
            rewritten.add(mapping[x]);
          }
        }
        if (!rewritten.isEmpty() && !list.contains(rewritten)) {
          list.add(rewritten);
        }
      }
      groupingSet = new GroupingSetSpec.Indices(list);
    }
    // marker to mimic group-by style handling in cardinality aggregator
    if (groupingSet == null) {
      groupingSet = GroupingSetSpec.EMPTY;
    }
    List<DimensionSpec> fields = Lists.newArrayList();
    for (String column : candidate) {
      fields.add(query.getDimensions().get(outputNames.indexOf(column)));
    }

    AggregatorFactory cardinality = CardinalityAggregatorFactory.dimensions("$cardinality", fields, groupingSet);
    if (query.getFilter() != null) {
      Map<String, String> mapping = QueryUtils.aliasMapping(this);
      if (!mapping.isEmpty()) {
        DimFilter redirected = query.getFilter().withRedirection(mapping);
        if (redirected != query.getFilter()) {
          query = query.withFilter(redirected);
        }
      }
    }

    return new TimeseriesQuery.Builder()
        .dataSource(query.getDataSource())
        .intervals(query.getQuerySegmentSpec())
        .descending(query.isDescending())
        .filters(query.getFilter())
        .granularity(query.getGranularity())
        .virtualColumns(query.getVirtualColumns())
        .aggregators(cardinality)
        .setContext(BaseQuery.copyContextForMeta(query.getContext()))
        .addContext(FINALIZE, true)
        .addContext(POST_PROCESSING, new PostProcessingOperator.ReturnsRow<Row>()
        {
          @Override
          public QueryRunner<Row> postProcess(final QueryRunner<Row> baseRunner)
          {
            return new QueryRunner<Row>()
            {
              @Override
              public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
              {
                Sequence<Row> sequence = baseRunner.run(query, responseContext);
                MutableInt sum = sequence.accumulate(new MutableInt(), new Accumulator<MutableInt, Row>()
                {
                  @Override
                  public MutableInt accumulate(MutableInt accumulated, Row in)
                  {
                    accumulated.add(Ints.checkedCast(in.getLongMetric("$cardinality")));
                    return accumulated;
                  }
                });
                return Sequences.<Row>of(
                    new MapBasedRow(0, ImmutableMap.<String, Object>of("cardinality", sum.intValue()))
                );
              }
            };
          }

          @Override
          public String toString()
          {
            return "cardinality_estimator";
          }
        })
        .build();
  }

  private Query<Row> toWorstCase(GroupByQuery query, ObjectMapper jsonMapper)
  {
    return PostProcessingOperators.append(
        query.withLimitSpec(query.getLimitSpec().withNoLimiting()), SequenceCountingProcessor.INSTANCE
    );
  }
}
