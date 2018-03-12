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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.DataSource;
import io.druid.query.Druids;
import io.druid.query.LateralViewSpec;
import io.druid.query.PostProcessingOperator;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.query.TimeseriesToRow;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.StringComparators;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.VirtualColumn;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
public class GroupByQuery extends BaseAggregationQuery<Row> implements Query.RewritingQuery<Row>
{
  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(BaseAggregationQuery<?> aggregationQuery)
  {
    return new Builder(aggregationQuery);
  }

  private final List<DimensionSpec> dimensions;

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
    for (DimensionSpec spec : this.dimensions) {
      Preconditions.checkArgument(spec != null, "dimensions has null DimensionSpec");
    }
    Queries.verifyAggregations(
        DimensionSpecs.toOutputNames(getDimensions()), getAggregatorSpecs(), getPostAggregatorSpecs()
    );
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
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
  public GroupByQuery withDataSource(DataSource dataSource)
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

  @Override
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

  @Override
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

  @Override
  public BaseAggregationQuery withLateralView(LateralViewSpec lateralView)
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
  public Query rewriteQuery(
      QuerySegmentWalker segmentWalker, QueryConfig queryConfig, ObjectMapper jsonMapper
  )
  {
    GroupByQuery query = this;
    if (query.getContextBoolean(GBY_PRE_ORDERING, queryConfig.groupBy.isPreOrdering())) {
      query = query.tryPreOrdering();
    }
    if (query.getContextBoolean(GBY_REMOVE_ORDERING, queryConfig.groupBy.isRemoveOrdering())) {
      query = tryRemoveOrdering(query);
    }
    if (query.getContextBoolean(GBY_LIMIT_PUSHDOWN, queryConfig.groupBy.isLimitPushdown())) {
      query = query.tryPushdown(segmentWalker, queryConfig, jsonMapper);
    }
    if (query.getContextBoolean(GBY_CONVERT_TIMESERIES, queryConfig.groupBy.isConvertTimeseries())) {
      return query.tryConvertToTimeseries(jsonMapper);
    }
    return query;
  }

  private GroupByQuery tryPreOrdering()
  {
    GroupByQuery query = this;
    if (!query.getContextBoolean(Query.GBY_MERGE_SIMPLE, true)) {
      return query;     // cannot apply
    }
    List<DimensionSpec> dimensionSpecs = query.getDimensions();
    if (dimensionSpecs.isEmpty()) {
      return query;
    }
    LimitSpec limitSpec = query.getLimitSpec();
    List<WindowingSpec> windowingSpecs = Lists.newArrayList(limitSpec.getWindowingSpecs());
    List<OrderByColumnSpec> orderingSpecs = Lists.newArrayList(limitSpec.getColumns());
    if (windowingSpecs.isEmpty() && orderingSpecs.isEmpty()) {
      return query;
    }
    if (!windowingSpecs.isEmpty()) {
      WindowingSpec first = windowingSpecs.get(0);
      orderingSpecs = first.asExpectedOrdering();
      List<DimensionSpec> rewritten = applyExplicitOrdering(orderingSpecs, dimensionSpecs);
      if (rewritten != null) {
        windowingSpecs.set(0, first.withoutOrdering());
        query = query.withLimitSpec(LimitSpecs.withWindowing(limitSpec, windowingSpecs))
                     .withDimensionSpecs(rewritten);
      }
    } else {
      List<DimensionSpec> rewritten = applyExplicitOrdering(orderingSpecs, dimensionSpecs);
      if (rewritten != null) {
        query = query.withLimitSpec(LimitSpecs.withOrderingSpec(limitSpec, null))
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
      if (!orderBy.isNaturalOrdering()) {
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

  private GroupByQuery tryRemoveOrdering(GroupByQuery query)
  {
    LimitSpec limitSpec = query.getLimitSpec();
    if (GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs()) &&
        LimitSpecs.isGroupByOrdering(limitSpec.getColumns(), query.getDimensions())) {
      query = query.withLimitSpec(LimitSpecs.of(limitSpec.getLimit()));
    }
    return query;
  }

  @SuppressWarnings("unchecked")
  private GroupByQuery tryPushdown(QuerySegmentWalker segmentWalker, QueryConfig queryConfig, ObjectMapper jsonMapper)
  {
    GroupByQuery query = this;
    LimitSpec limitSpec = query.getLimitSpec();
    List<DimensionSpec> dimensionSpecs = query.getDimensions();
    if (limitSpec.getLimit() > queryConfig.groupBy.getLimitPushdownThreshold()) {
      return query;
    }
    if (!GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs())) {
      return query;
    }
    if (!LimitSpecs.isGroupByOrdering(limitSpec.getColumns(), dimensionSpecs)) {
      return query;
    }
    if (query.getGranularity() != Granularities.ALL) {
      return query;  // todo
    }
    List<String> dimensions = DimensionSpecs.toInputNames(dimensionSpecs);
    if (dimensions.isEmpty()) {
      return query;
    }
    final char separator = '\u0001';

    List<VirtualColumn> vcs = null;
    Map<String, Object> ag;
    Map<String, Object> pg = ImmutableMap.<String, Object>builder()
                                         .put("type", "sketch.quantiles")
                                         .put("name", "SPLIT")
                                         .put("fieldName", "SKETCH")
                                         .put("op", "QUANTILES_CDF")
                                         .put("slopedSpaced", 31)
                                         .put("ratioAsCount", true)
                                         .build();

    if (dimensions.size() == 1) {
      String dimension = dimensions.get(0);
      ag = ImmutableMap.<String, Object>builder()
                       .put("type", "sketch")
                       .put("name", "SKETCH")
                       .put("fieldName", dimension)
                       .put("sketchOp", "QUANTILE")
                       .put("sketchParam", 128)
                       .build();
    } else {
      String comparator = StringComparators.asComparatorName(separator, dimensionSpecs);
      vcs = Arrays.<VirtualColumn>asList(
          new ExprVirtualColumn("concat(" + StringUtils.join(dimensions, ", '" + separator + "',") + ")", "VC")
      );
      ag = ImmutableMap.<String, Object>builder()
                       .put("type", "sketch")
                       .put("name", "SKETCH")
                       .put("fieldName", "VC")
                       .put("sketchOp", "QUANTILE")
                       .put("sketchParam", 128)
                       .put("stringComparator", comparator)
                       .build();
    }
    List<Direction> directions = DimensionSpecs.getDirections(dimensionSpecs);

    TimeseriesQuery metaQuery = new TimeseriesQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        query.isDescending(),
        query.getDimFilter(),
        query.getGranularity(),
        vcs,
        Arrays.asList(Queries.convert(ag, jsonMapper, AggregatorFactory.class)),
        Arrays.asList(Queries.convert(pg, jsonMapper, PostAggregator.class)),
        null,
        null,
        Arrays.asList("SPLIT"),
        null,
        ImmutableMap.<String, Object>copyOf(getContext())
    );
    Result<TimeseriesResultValue> result = Iterables.getOnlyElement(
        Sequences.toList(
            metaQuery.run(segmentWalker, Maps.<String, Object>newHashMap()),
            Lists.<Result<TimeseriesResultValue>>newArrayList()
        ), null
    );
    if (result == null) {
      return query;
    }
    Logger logger = new Logger(GroupByQuery.class);

    // made in broker.. keeps type (not "list of numbers" for cdf)
    Map<String, Object> value = (Map<String, Object>) result.getValue().getMetric("SPLIT");
    String[] values = (String[])value.get("splits");
    long[] counts = (long[])value.get("cdf");
    if (values == null || counts == null) {
      return query;
    }

    logger.info("--> values : " + Arrays.toString(values));
    logger.info("--> counts : " + Arrays.toString(counts));

    long limit = (long) (limitSpec.getLimit() * 1.1);
    int index = Arrays.binarySearch(counts, limit);
    if (index < 0) {
      index = -index - 1;
    }
    logger.info("--> " + limit + "  : " + index);
    if (index == counts.length) {
      return query;
    }
    String[] minValues = Preconditions.checkNotNull(StringUtils.split(values[0], separator));
    String[] splits = Preconditions.checkNotNull(StringUtils.split(values[index], separator));
    if (dimensions.size() != splits.length) {
      return query;
    }
    logger.info("--> split : " + Arrays.toString(splits));

    StringBuilder builder = new StringBuilder();

    int i = 0;
    for (; splits[i].equals(minValues[i]) && i < splits.length; i++) {
      builder.append(dimensions.get(i)).append(" == ").append('\'').append(minValues[i]).append('\'');
    }
    if (i < splits.length) {
      if (builder.length() > 0) {
        builder.append(" && ");
      }
      String x = directions.get(i) == Direction.ASCENDING ? " <= " : " >= ";
      builder.append(dimensions.get(i)).append(x).append('\'').append(splits[i]).append('\'');
    }
    logger.info("---------> " + builder.toString());
    if (builder.length() == 0) {
      return query;
    }

    DimFilter newFilter = AndDimFilter.of(query.getDimFilter(), new MathExprFilter(builder.toString()));
    logger.info("---------> " + newFilter);
    return query.withDimFilter(newFilter);
  }

  private Query tryConvertToTimeseries(ObjectMapper jsonMapper)
  {
    if (!dimensions.isEmpty() || needsSchemaResolution()) {
      return this;
    }
    PostProcessingOperator current = PostProcessingOperators.load(this, jsonMapper);
    if (current == null) {
      current = new TimeseriesToRow();
    }
    return Druids.newTimeseriesQueryBuilder().copy(this)
                 .overrideContext(ImmutableMap.<String, Object>of(POST_PROCESSING, current))
                 .build();
  }

  public static class Builder extends BaseAggregationQuery.Builder<GroupByQuery>
  {
    public Builder() { }

    public Builder(BaseAggregationQuery<?> aggregationQuery)
    {
      super(aggregationQuery);
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
  public String toString()
  {
    return "GroupByQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", granularity=" + granularity +
           ", dimensions=" + dimensions +
           (dimFilter == null ? "" : ", dimFilter=" + dimFilter) +
           (virtualColumns.isEmpty() ? "" : ", virtualColumns=" + virtualColumns) +
           (aggregatorSpecs.isEmpty() ? "" : ", aggregatorSpecs=" + aggregatorSpecs) +
           (postAggregatorSpecs.isEmpty() ? "" : ", postAggregatorSpecs=" + postAggregatorSpecs) +
           (havingSpec == null ? "" : ", havingSpec=" + havingSpec) +
           (limitSpec == null ? "" : ", limitSpec=" + limitSpec) +
           (outputColumns == null ? "" : ", outputColumns=" + outputColumns) +
           (lateralView == null ? "" : "lateralView" + lateralView) +
           ", context=" + getContext() +
           '}';
  }

  @Override
  @SuppressWarnings("unchecked")
  public Ordering getResultOrdering()
  {
    final Comparator naturalNullsFirst = Ordering.natural().nullsFirst();
    final Ordering<Row> rowOrdering = getRowOrdering();

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

  Ordering<Row> getRowOrdering()
  {
    final String[] outputNames = new String[dimensions.size()];
    for (int i = 0; i < outputNames.length; i++) {
      outputNames[i] = dimensions.get(i).getOutputName();
    }
    return getRowOrdering(outputNames);
  }

  static final Comparator<Object> naturalNullsFirst = GuavaUtils.nullFirstNatural();

  static Ordering<Row> getRowOrdering(final String[] outputNames)
  {
    return Ordering.from(
        new Comparator<Row>()
        {
          @Override
          public int compare(Row lhs, Row rhs)
          {
            final int timeCompare = Longs.compare(
                lhs.getTimestampFromEpoch(),
                rhs.getTimestampFromEpoch()
            );

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
