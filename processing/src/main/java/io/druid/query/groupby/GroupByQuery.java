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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.LateralViewSpec;
import io.druid.query.PostProcessingOperator;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.Result;
import io.druid.query.SequenceCountingProcessor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.InDimFilter;
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
import io.druid.segment.VirtualColumn;

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
      @JsonProperty("filter") DimFilter dimFilter,
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
        dimFilter,
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
        dimFilter,
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
        dimFilter,
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
  public GroupByQuery withDimFilter(final DimFilter dimFilter)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimFilter,
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
        dimFilter,
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
        getDimFilter(),
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
        getDimFilter(),
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
        getDimFilter(),
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
        getDimFilter(),
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
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
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

  @Override
  public GroupByQuery withOutputColumns(final List<String> outputColumns)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
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
        getDimFilter(),
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
        getDimFilter(),
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
        getDimFilter(),
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
        getDimFilter(),
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
  public GroupByQuery removePostActions()
  {
    Map<String, Object> override = defaultPostActionContext();
    override.put(LOCAL_SPLIT_STRATEGY, getLocalSplitStrategy());
    override.put(FUDGE_TIMESTAMP, Objects.toString(GroupByQueryEngine.getUniversalTimestamp(this), null));

    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getGroupingSets(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        null,
        null,
        getLimitSpec().withNoProcessing(),
        null,
        null,
        computeOverriddenContext(override)
    );
  }

  @Override
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    GroupByQuery query = this;
    GroupByQueryConfig groupByConfig = queryConfig.getGroupBy();
    if (query.getContextBoolean(GBY_PRE_ORDERING, groupByConfig.isPreOrdering())) {
      query = query.tryPreOrdering();
    }
    if (query.getContextBoolean(GBY_REMOVE_ORDERING, groupByConfig.isRemoveOrdering())) {
      query = query.tryRemoveOrdering();
    }
    if (query.getContextBoolean(GBY_CONVERT_TIMESERIES, groupByConfig.isConvertTimeseries())) {
      Query converted = query.tryConvertToTimeseries(segmentWalker.getObjectMapper());
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
    List<DimensionSpec> dimensionSpecs = query.getDimensions();
    if (dimensionSpecs.isEmpty()) {
      return query;
    }
    LimitSpec limitSpec = query.getLimitSpec();
    if (!GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs())) {
      List<WindowingSpec> windowingSpecs = Lists.newArrayList(limitSpec.getWindowingSpecs());
      WindowingSpec first = windowingSpecs.get(0);
      List<DimensionSpec> rewritten = applyExplicitOrdering(first.getPartitionOrdering(), dimensionSpecs);
      if (rewritten != null) {
        windowingSpecs.set(0, first.withoutOrdering());
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
    if (GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs()) &&
        LimitSpecs.inGroupByOrdering(limitSpec.getColumns(), dimensions)) {
      return withLimitSpec(LimitSpecs.of(limitSpec.getLimit()));
    }
    return this;
  }

  private Query tryConvertToTimeseries(ObjectMapper jsonMapper)
  {
    if (!dimensions.isEmpty()) {
      return null;
    }
    return Druids.newTimeseriesQueryBuilder().copy(this).build();
  }

  private Query tryEstimateTopN(QuerySegmentWalker segmentWalker, int estimationFactor)
  {
    if (getGroupingSets() != null || getGranularity() != Granularities.ALL) {
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
    new TopNQuery(
        getDataSource(),
        getVirtualColumns(),
        dimensionSpec,
        metricSpec,
        limitSpec.getLimit() * estimationFactor,
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        condensed.lhs,
        condensed.rhs,
        getOutputColumns(),
        BaseQuery.copyContextForMeta(this)
    ).run(segmentWalker, Maps.<String, Object>newHashMap()).accumulate(
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
    DimFilter filter = new InDimFilter(
        dimensionSpec.getDimension(),
        Lists.newArrayList(dimensionValues),
        dimensionSpec.getExtractionFn()
    );
    // seemed not need to split now
    return withDimFilter(DimFilters.and(filter, getDimFilter())).withOverriddenContext(GBY_LOCAL_SPLIT_NUM, -1);
  }

  public int[][] getGroupings()
  {
    return groupingSets == null ? new int[0][0] : groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensions));
  }

  public static class Builder extends BaseAggregationQuery.Builder<GroupByQuery>
  {
    private GroupingSetSpec groupingSets;

    public Builder() { }

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
           ", granularity=" + granularity +
           ", dimensions=" + dimensions +
           (dimFilter == null ? "" : ", dimFilter=" + dimFilter) +
           (groupingSets == null ? "" : ", groupingSets=" + groupingSets) +
           (virtualColumns.isEmpty() ? "" : ", virtualColumns=" + virtualColumns) +
           (aggregatorSpecs.isEmpty() ? "" : ", aggregatorSpecs=" + aggregatorSpecs) +
           (postAggregatorSpecs.isEmpty() ? "" : ", postAggregatorSpecs=" + postAggregatorSpecs) +
           (havingSpec == null ? "" : ", havingSpec=" + havingSpec) +
           (limitSpec == null ? "" : ", limitSpec=" + limitSpec) +
           (outputColumns == null ? "" : ", outputColumns=" + outputColumns) +
           (lateralView == null ? "" : "lateralView=" + lateralView) +
           (GuavaUtils.isNullOrEmpty(getContext()) ? "" : ", context=" + getContext()) +
           '}';
  }

  @Override
  public Ordering<Row> getResultOrdering()
  {
    return isBySegment(this) ? GuavaUtils.<Row>nullFirstNatural() : getRowOrdering();
  }

  @SuppressWarnings("unchecked")
  private Ordering<Row> getRowOrdering()
  {
    return Ordering.from(
        new Comparator<Row>()
        {
          private final Comparator[] comparators = DimensionSpecs.toComparator(getDimensions(), true);

          @Override
          public int compare(Row lhs, Row rhs)
          {
            final Object[] values1 = ((CompactRow) lhs).getValues();
            final Object[] values2 = ((CompactRow) rhs).getValues();
            int compare = 0;
            for (int i = 0; compare == 0 && i < comparators.length; i++) {
              compare = comparators[i].compare(values1[i], values2[i]);
            }
            return compare;
          }
        }
    );
  }

  public TimeseriesQuery asTimeseriesQuery()
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
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
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        BaseQuery.copyContextForMeta(this)
    );
  }

  public Query<Row> toCardinalityEstimator(QueryConfig config, ObjectMapper mapper, boolean throwException)
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
    List<String> metrics = GuavaUtils.concat(
        AggregatorFactory.toNames(query.getAggregatorSpecs()),
        PostAggregators.toNames(query.getPostAggregatorSpecs())
    );
    LimitSpec limitSpec = query.getLimitSpec();
    List<String> candidate = outputNames;
    if (limitSpec != null && !GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs())) {
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
      groupingSet = new GroupingSetSpec.Indices(null);
    }
    List<DimensionSpec> fields = Lists.newArrayList();
    for (String column : candidate) {
      fields.add(query.getDimensions().get(outputNames.indexOf(column)));
    }

    // todo: is this right?
    boolean sortOnTime = query.isSortOnTimeForLimit(config.getGroupBy().isSortOnTime());
    Granularity granularity = sortOnTime ? query.getGranularity() : Granularities.ALL;

    AggregatorFactory cardinality = new CardinalityAggregatorFactory(
        "$cardinality", null, fields, groupingSet, null, true, true
    );
    if (query.getDimFilter() != null) {
      Map<String, String> mapping = QueryUtils.aliasMapping(this);
      if (!mapping.isEmpty()) {
        query = query.withDimFilter(query.getDimFilter().withRedirection(mapping));
      }
    }

    return query.withGroupingSet(null)
                .withPostAggregatorSpecs(null)
                .withAggregatorSpecs(Arrays.asList(cardinality))
                .withGranularity(granularity)
                .withDimensionSpecs(null)
                .withHavingSpec(null)
                .withLimitSpec(null)
                .withOutputColumns(null)
                .withOverriddenContext(FINALIZE, true)
                .withOverriddenContext(FINAL_MERGE, true)
                .withOverriddenContext(GBY_CONVERT_TIMESERIES, true)
                .withOverriddenContext(ALL_DIMENSIONS_FOR_EMPTY, false)
                .withOverriddenContext(POST_PROCESSING, new PostProcessingOperator.Abstract<Row>()
                {
                  @Override
                  public QueryRunner<Row> postProcess(final QueryRunner<Row> baseRunner)
                  {
                    return new QueryRunner<Row>()
                    {
                      @Override
                      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
                      {
                        int sum = baseRunner.run(query, responseContext).accumulate(0, new Accumulator<Integer, Row>()
                        {
                          @Override
                          public Integer accumulate(Integer accumulated, Row in)
                          {
                            return accumulated + Ints.checkedCast(in.getLongMetric("$cardinality"));
                          }
                        });
                        return Sequences.<Row>of(
                            new MapBasedRow(0, ImmutableMap.<String, Object>of("cardinality", sum))
                        );
                      }

                      @Override
                      public String toString()
                      {
                        return "cardinality";
                      }
                    };
                  }
                });
  }

  private Query<Row> toWorstCase(GroupByQuery query, ObjectMapper jsonMapper)
  {
    return PostProcessingOperators.append(
        query.withLimitSpec(query.getLimitSpec().withNoLimiting()), jsonMapper, SequenceCountingProcessor.INSTANCE
    );
  }
}
