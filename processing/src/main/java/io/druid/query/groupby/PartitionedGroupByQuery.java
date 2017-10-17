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
import com.google.common.collect.Maps;
import com.google.common.math.IntMath;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
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
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import org.joda.time.Interval;

import java.math.RoundingMode;
import java.util.Arrays;
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
      @JsonProperty("limitSpec") LimitSpec limitSpec,
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
        limitSpec == null ? null : limitSpec.withLimit(Integer.MAX_VALUE),
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
    Preconditions.checkArgument(
        getGranularity() == QueryGranularities.ALL || numPartition > 0,
        "if 'granularity' is not 'ALL', only 'numPartition' can be applicable"
    );
    this.limit = limitSpec == null ? Integer.MAX_VALUE : limitSpec.getLimit();
    this.numPartition = numPartition;
    this.scannerLen = scannerLen;
    this.parallelism = parallelism;
    this.queue = queue;
    if (numPartition != 1 && limitSpec != null && limitSpec.getWindowingSpecs() != null) {
      String partitioned = getDimensions().get(0).getDimension();
      for (WindowingSpec spec : limitSpec.getWindowingSpecs()) {
        List<String> partitionColumns = spec.getPartitionColumns();
        Preconditions.checkArgument(
            partitionColumns == null || partitionColumns.isEmpty() || partitioned.equals(partitionColumns.get(0))
        );
      }
    }
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
    if (numPartition == 1) {
      return asGroupByQuery(null, null);
    }
    QueryGranularity granularity = getGranularity();
    if (granularity == QueryGranularities.ALL) {
      return splitOnDimension(null, numPartition, segmentWalker, jsonMapper);
    }
    // split first on time.. only possible to use numPartition
    Preconditions.checkArgument(numPartition > 1);

    // get real interval.. executed in local (see CCC)
    List<Interval> analyzed = QueryUtils.analyzeInterval(segmentWalker, this);
    List<Interval> splits = Lists.newArrayList();
    for (Interval interval : getQuerySegmentSpec().getIntervals()) {
      Interval trimmed = JodaUtils.trim(interval, analyzed);
      if (trimmed != null) {
        Iterables.addAll(splits, QueryGranularities.split(trimmed, granularity));
      }
    }
    List<Query> queries = Lists.newArrayList();
    if (splits.size() < numPartition / 2) {
      // split more on dimension
      int numSubPartition = numPartition / splits.size();
      for (Interval interval : splits) {
        queries.add(splitOnDimension(Arrays.asList(interval), numSubPartition, segmentWalker, jsonMapper));
      }
    } else {
      // just split on time
      RoundingMode mode = splits.size() < numPartition ? RoundingMode.FLOOR : RoundingMode.CEILING;
      int partitionSize = IntMath.divide(splits.size(), numPartition, mode);
      for (List<Interval> partition : Lists.partition(splits, partitionSize)) {
        queries.add(asGroupByQuery(partition, null));
      }
    }
    return new UnionAllQuery(null, queries, false, limit, parallelism, queue, getContext());
  }

  @SuppressWarnings("unchecked")
  private Query splitOnDimension(
      List<Interval> intervals,
      int numPartition,
      QuerySegmentWalker segmentWalker,
      ObjectMapper jsonMapper
  )
  {
    DimensionSpec dimensionSpec = getDimensions().get(0);
    String dimension = dimensionSpec.getDimension();
    List<String> partitions = QueryUtils.runSketchQuery(
        segmentWalker, jsonMapper, getQuerySegmentSpec(), getDimFilter(),
        getDataSource(), dimension, numPartition, scannerLen
    );
    if (partitions == null || partitions.size() == 1) {
      return asGroupByQuery(intervals, null);
    }
    List<Query> queries = Lists.newArrayList();
    for (DimFilter filter : QueryUtils.toFilters(dimension, partitions)) {
      queries.add(asGroupByQuery(intervals, filter));
    }
    return new GroupByDelegate(queries, limit, parallelism, queue, Maps.newHashMap(getContext()));
  }

  private GroupByQuery asGroupByQuery(List<Interval> interval, DimFilter filter)
  {
    DimFilter current = getDimFilter();
    return new GroupByQuery(
        getDataSource(),
        interval == null ? getQuerySegmentSpec() : new MultipleIntervalSegmentSpec(interval),
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
        Maps.newHashMap(getContext())
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
        getLimitSpec(),
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

  @Override
  public PartitionedGroupByQuery withDimensionSpecs(final List<DimensionSpec> dimensionSpecs)
  {
    return new PartitionedGroupByQuery(
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
        numPartition,
        scannerLen,
        parallelism,
        queue,
        getContext()
    );
  }

  @Override
  public PartitionedGroupByQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new PartitionedGroupByQuery(
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
        numPartition,
        scannerLen,
        parallelism,
        queue,
        getContext()
    );
  }

  @SuppressWarnings("unchecked")
  public static class GroupByDelegate<T extends Comparable<T>> extends UnionAllQuery<T>
  {
    public GroupByDelegate(List<Query<T>> list, int limit, int parallelism, int queue, Map<String, Object> context)
    {
      super(null, list, false, limit, parallelism, queue, context);
    }

    @Override
    public Query withQueries(List queries)
    {
      return new GroupByDelegate(queries, getLimit(), getParallelism(), getQueue(), getContext());
    }

    @Override
    public Query withQuery(Query query)
    {
      throw new IllegalStateException();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Query<T> newInstance(Query<T> query, List<Query<T>> queries, Map<String, Object> context)
    {
      Preconditions.checkArgument(query == null);
      return new GroupByDelegate(queries, getLimit(), getParallelism(), getQueue(), context);
    }

    @Override
    public String toString()
    {
      return "GroupByDelegate{" +
             "queries=" + getQueries() +
             ", parallelism=" + getParallelism() +
             ", queue=" + getQueue() +
             ", limit=" + getLimit() +
             '}';
    }
  }
}
