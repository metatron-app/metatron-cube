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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.collections.StupidPool;
import io.druid.common.guava.Accumulator;
import io.druid.common.guava.CombiningSequence;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.GroupByMergedQueryRunner;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.QueryWatcher;
import io.druid.query.RowResolver;
import io.druid.query.StreamAggregationFn;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.select.StreamQueryEngine;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.Cuboids;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class GroupByQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<Row, GroupByQuery>
    implements QueryRunnerFactory.Splitable<Row, GroupByQuery>
{
  private static final Logger logger = new Logger(GroupByQueryRunnerFactory.class);

  private static final int MAX_LOCAL_SPLIT = 64;

  private final GroupByQueryEngine engine;
  private final StreamQueryEngine stream;
  private final QueryConfig config;

  @Inject
  public GroupByQueryRunnerFactory(
      GroupByQueryEngine engine,
      StreamQueryEngine stream,
      QueryWatcher queryWatcher,
      QueryConfig config,
      GroupByQueryQueryToolChest toolChest,
      @Global StupidPool<ByteBuffer> computationBufferPool
  )
  {
    super(toolChest, queryWatcher);
    this.engine = engine;
    this.stream = stream;
    this.config = config;
  }

  @Override
  public Supplier<Object> preFactoring(
      GroupByQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    List<ValueType> types = Lists.newArrayList();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      ValueDesc type = dimensionSpec.resolve(resolver);
      if (type.isMap()) {
        String[] descriptiveType = type.getDescription();
        if (descriptiveType == null) {
          throw new ISE("cannot resolve value type of map %s [%s]", type, dimensionSpec);
        }
        type = ValueDesc.of(descriptiveType[2]);
      }
      if (type.isDimension() || type.isMultiValued()) {
        types.add(type.subElement(ValueDesc.STRING).type());
      } else if (type.isArray()) {
        types.add(type.subElement(ValueDesc.UNKNOWN).type());
      } else if (type.isPrimitive()) {
        types.add(type.type());
      } else if (type.isBitSet()) {
        types.add(ValueType.STRING);
      } else {
        throw new ISE("cannot group-by on non-primitive type %s [%s]", type, dimensionSpec);
      }
    }
    return Suppliers.ofInstance(types);
  }

  @Override
  public List<List<Segment>> splitSegments(
      GroupByQuery query,
      List<Segment> segments,
      Supplier<Object> optimizer,
      Supplier<RowResolver> resolver,
      QuerySegmentWalker segmentWalker
  )
  {
    // this possibly does not reduce total cardinality to handle..
    if (Granularities.ALL.equals(query.getGranularity())) {
      return null;  // cannot split on time
    }
    if (query.getDimensions().isEmpty()) {
      return null;  // use timeseries query
    }
    if (segments.size() <= 1) {
      return null;  // nothing to split (actually can.. later)
    }
    GroupByQueryConfig gbyConfig = config.getGroupBy();
    int numSplit = query.getContextInt(Query.GBY_LOCAL_SPLIT_NUM, gbyConfig.getLocalSplitNum());
    List<List<Segment>> segmentGroups = Lists.newArrayList();
    if (numSplit <= 0) {
      int maxCardinality = query.getContextInt(
          Query.GBY_LOCAL_SPLIT_CARDINALITY,
          gbyConfig.getLocalSplitCardinality()
      );
      if (maxCardinality < 0) {
        return null;
      }
      AggregatorFactory aggregator = CardinalityAggregatorFactory.dimensions(
          "cardinality", query.getDimensions(), query.getGroupingSets()
      );
      TimeseriesQuery timeseries = query.asTimeseriesQuery()
                                        .withPostAggregatorSpecs(null)
                                        .withAggregatorSpecs(Arrays.asList(aggregator))
                                        .withOverriddenContext(Query.FINALIZE, false);

      final Group group = new Group();
      for (Segment segment : segments) {
        Sequence<Row> sequence = QueryRunners.run(Segments.prepare(timeseries, segment), segmentWalker);
        long cardinality = sequence.accumulate(new MutableLong(), new Accumulator<MutableLong, Row>()
        {
          @Override
          public MutableLong accumulate(MutableLong accumulated, Row row)
          {
            accumulated.add(((HyperLogLogCollector) row.getRaw("cardinality")).estimateCardinalityRound());
            return accumulated;
          }
        }).longValue();
        if (group.numRows + cardinality < maxCardinality ||
            group.interval != null && segment.getInterval().overlaps(group.interval)) {
          group.add(segment, cardinality);
        } else if (group.segments.isEmpty()) {
          segmentGroups.add(Arrays.asList(segment));
        } else {
          segmentGroups.add(ImmutableList.copyOf(group.segments));
          group.reset().add(segment, cardinality);
        }
      }
      if (!group.segments.isEmpty()) {
        segmentGroups.add(ImmutableList.copyOf(group.segments));
      }
    } else {
      int segmentSize = segments.size() / numSplit;
      segmentGroups = segmentSize <= 1 ? ImmutableList.<List<Segment>>of() : Lists.partition(segments, segmentSize);
    }
    return segmentGroups.size() <= 1 ? null : segmentGroups;
  }

  private static class Group
  {
    private final List<Segment> segments = Lists.newArrayList();
    private Interval interval;
    private long numRows;

    private Group add(Segment segment, long cardinality)
    {
      segments.add(segment);
      numRows += cardinality;
      interval = segment.getInterval();
      return this;
    }

    public Group reset()
    {
      segments.clear();
      numRows = 0;
      interval = null;
      return this;
    }
  }

  @Override
  public List<GroupByQuery> splitQuery(
      GroupByQuery query,
      List<Segment> segments,
      Supplier<Object> optimizer,
      Supplier<RowResolver> resolver,
      QuerySegmentWalker segmentWalker
  )
  {
    if (!Granularities.ALL.equals(query.getGranularity()) || BaseQuery.isBySegment(query)) {
      return null;  // cannot split on column
    }
    if (query.getLimitSpec().getSegmentLimit() != null) {
      return null;  // not sure of this
    }
    if (query.getLimitSpec().getNodeLimit() != null) {
      return null;  // todo
    }
    List<DimensionSpec> dimensionSpecs = query.getDimensions();
    if (dimensionSpecs.isEmpty()) {
      return null;  // use timeseries query
    }
    GroupByQueryConfig gbyConfig = config.getGroupBy();
    int maxResults = config.getMaxResults(query);
    int numSplit = query.getContextInt(Query.GBY_LOCAL_SPLIT_NUM, gbyConfig.getLocalSplitNum());
    if (numSplit < 0) {
      int splitCardinality = query.getContextInt(Query.GBY_LOCAL_SPLIT_CARDINALITY, gbyConfig.getLocalSplitCardinality());
      if (splitCardinality > 1) {
        splitCardinality = Math.min(splitCardinality, maxResults);
        long start = System.currentTimeMillis();
        long cardinality = Queries.estimateCardinality(query, segments, segmentWalker, splitCardinality);
        if (cardinality <= 0) {
          return null;    // failed ?
        }
        long elapsed = System.currentTimeMillis() - start;
        numSplit = (int) Math.ceil(cardinality * 1.2 / splitCardinality);
        if (numSplit > 1) {
          logger.info("Expected cardinality %d, split into %d queries (%d msec)", cardinality, numSplit, elapsed);
        } else {
          logger.info("Expected cardinality %d. no split (%d msec)", cardinality, elapsed);
        }
        if (query.isStreamingAggregatable() && cardinality > splitCardinality * 0.5 &&
            query.getContextBoolean(Query.GBY_STREAMING, gbyConfig.isStreamingAggregation())) {
          long[] selectivity = Queries.estimateSelectivity(query, segmentWalker);
          if (selectivity[0] > 0 && cardinality > selectivity[0] * 0.25) {
            logger.info("Using streaming aggregation.. ratio = [%.2f]", cardinality / (float) selectivity[0]);
            query = query.withOverriddenContext(Query.STREAMING_GBY_SCHEMA, resolver.get().resolve(query.toStreaming()));
          }
        }
      }
    }
    if (numSplit > MAX_LOCAL_SPLIT) {
      throw new ISE("Too many splits %,d", numSplit);
    } else if (numSplit < 2) {
      return null;
    }

    // can split on all dimensions but it seemed not cost-effective
    DimensionSpec dimensionSpec = dimensionSpecs.get(0);

    long start = System.currentTimeMillis();
    String strategy = query.getContextValue(Query.LOCAL_SPLIT_STRATEGY, "slopedSpaced");
    Object[] thresholds = Queries.makeColumnHistogramOn(
        resolver, segments, segmentWalker, query, dimensionSpec, numSplit, strategy, maxResults, cache(query)
    );
    if (thresholds == null || thresholds.length < 3) {
      return null;
    }
    long elapsed = System.currentTimeMillis() - start;
    logger.info("split %s on values : %s (%d msec)", dimensionSpec.getDimension(), Arrays.toString(thresholds), elapsed);

    ValueDesc type = dimensionSpec.resolve(resolver).unwrapDimension();
    Map<String, String> mapping = QueryUtils.aliasMapping(query);
    OrderByColumnSpec orderingSpec = DimensionSpecs.asOrderByColumnSpec(dimensionSpec);
    String dimension = mapping.getOrDefault(dimensionSpec.getOutputName(), dimensionSpec.getOutputName());

    List<GroupByQuery> splits = Lists.newArrayList();
    for (DimFilter filter : QueryUtils.toSplitter(dimension, orderingSpec, thresholds)) {
      logger.debug("--> split filter : %s", filter);
      splits.add(query.withFilter(DimFilters.and(query.getFilter(), filter)));
    }
    return splits;
  }

  @Override
  public QueryRunner<Row> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return (query, response) ->
    {
      GroupByQuery groupBy = (GroupByQuery) query;
      if (groupBy.getContextValue(Query.STREAMING_GBY_SCHEMA) != null) {
        Sequence<Object[]> sequence = stream.process(groupBy.toStreaming(), config, segment, null, cache);
        Long fixedTimestamp = BaseQuery.getUniversalTimestamp(query, null);
        if (fixedTimestamp != null) {
          sequence = Sequences.peek(sequence, v -> v[0] = fixedTimestamp);
        }
        return Sequences.map(sequence, CompactRow::new);
      }
      if (groupBy.getContextBoolean(Query.USE_CUBOIDS, config.isUseCuboids())) {
        Segment cuboid = segment.cuboidFor(groupBy);
        if (cuboid != null) {
          return engine.process(Cuboids.rewrite(groupBy), config, cuboid, true, null);   // disable filter cache
        }
      }
      return engine.process(groupBy, config, segment, true, cache);
    };
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      final Query<Row> input,
      final ExecutorService exec,
      final Iterable<QueryRunner<Row>> runners,
      final Supplier<Object> optimizer
  )
  {
    return (query, response) ->
    {
      RowResolver resolver = query.getContextValue(Query.STREAMING_GBY_SCHEMA);
      if (resolver == null) {
        // mergeRunners should take ListeningExecutorService at some point
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(exec);
        return new GroupByMergedQueryRunner(executor, config, queryWatcher, runners).run(query, response);
      }
      GroupByQuery groupBy = (GroupByQuery) query;
      Sequence<Row> sequence = QueryRunners.executeParallel(groupBy, exec, Lists.newArrayList(runners), queryWatcher)
                                           .run(query, response);
      return CombiningSequence.create(
          sequence, groupBy.getCompactRowOrdering(), StreamAggregationFn.of(groupBy, resolver)
      );
    };
  }
}
