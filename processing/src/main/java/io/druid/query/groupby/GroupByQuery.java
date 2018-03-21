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
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
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
import io.druid.query.TimeseriesToRow;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

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

  private GroupByQuery tryRemoveOrdering(GroupByQuery query)
  {
    LimitSpec limitSpec = query.getLimitSpec();
    if (GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs()) &&
        LimitSpecs.isGroupByOrdering(limitSpec.getColumns(), query.getDimensions())) {
      query = query.withLimitSpec(LimitSpecs.of(limitSpec.getLimit()));
    }
    return query;
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
