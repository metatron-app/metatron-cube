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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.UnionAllQuery;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExpressionDimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;

/**
 */
public class PartitionedGroupByQuery extends GroupByQuery implements Query.RewritingQuery<Row>
{
  private final int limit;
  private final int numPartition;
  private final int scannerLen;
  private final int parallelism;
  private final int queue;

  public PartitionedGroupByQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("having") HavingSpec havingSpec,
      @JsonProperty("limit") int limit,
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("numPartition") int numPartition,
      @JsonProperty("scannerLen") int scannerLen,
      @JsonProperty("parallelism") int parallelism,
      @JsonProperty("queue") int queue,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource,
        querySegmentSpec,
        dimFilter,
        granularity,
        dimensions,
        virtualColumns,
        aggregatorSpecs,
        postAggregatorSpecs,
        havingSpec,
        limit > 0 ? new DefaultLimitSpec(null, limit, null) : null,
        outputColumns,
        null,
        context
    );
    Preconditions.checkArgument(!getDimensions().isEmpty());
    Preconditions.checkArgument(
        getDimensions().get(0) instanceof DefaultDimensionSpec ||
        getDimensions().get(0) instanceof ExpressionDimensionSpec
    );  // todo extraction
    Preconditions.checkArgument(
        numPartition > 0 || scannerLen > 0, "one of 'numPartition' or 'scannerLen' should be configured"
    );
    this.limit = limit;
    this.numPartition = numPartition;
    this.scannerLen = scannerLen;
    this.parallelism = parallelism;
    this.queue = queue;
  }

  @JsonProperty
  public int getNumPartition()
  {
    return numPartition;
  }

  @JsonProperty
  public int getScannerLen()
  {
    return scannerLen;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper)
  {
    String table = Iterables.getOnlyElement(getDataSource().getNames());
    DimensionSpec dimensionSpec = getDimensions().get(0);
    String expression = dimensionSpec.getDimension();
    if (dimensionSpec instanceof ExpressionDimensionSpec) {
      expression = ((ExpressionDimensionSpec) dimensionSpec).getExpression();
    }
    List<String> partitions = QueryUtils.runSketchQuery(
        segmentWalker, jsonMapper, getQuerySegmentSpec(), getDimFilter(),
        table, expression, numPartition, scannerLen
    );
    if (partitions == null || partitions.size() == 1) {
      return asGroupByQuery(null);
    }
    List<Query> queries = Lists.newArrayList();
    for (DimFilter filter : QueryUtils.toFilters(expression, partitions)) {
      queries.add(asGroupByQuery(filter));
    }
    return new UnionAllQuery(null, queries, false, limit, parallelism, queue, getContext());
  }

  private GroupByQuery asGroupByQuery(DimFilter filter)
  {
    DimFilter current = getDimFilter();
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter == null ? current : current != null ? AndDimFilter.of(current, filter) : filter,
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
  public String getType()
  {
    return Query.GROUP_BY_PARTITIONED;
  }

  @Override
  @SuppressWarnings("unchecked")
  public PartitionedGroupByQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new PartitionedGroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getVirtualColumns(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        getHavingSpec(),
        limit,
        getOutputColumns(),
        numPartition,
        scannerLen,
        parallelism,
        queue,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public PartitionedGroupByQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new IllegalStateException();
  }

  @Override
  public PartitionedGroupByQuery withDataSource(DataSource dataSource)
  {
    throw new IllegalStateException();
  }
}
