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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import io.druid.cache.Cache;
import io.druid.collections.StupidPool;
import io.druid.common.utils.Sequences;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.guice.annotations.Global;
import io.druid.query.GroupByMergedQueryRunner;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.OrderingSpec;
import io.druid.query.sketch.QuantileOperation;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.column.DictionaryEncodedColumn;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class GroupByQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<Row, GroupByQuery>
    implements QueryRunnerFactory.Splitable<Row, GroupByQuery>
{
  private static final Logger logger = new Logger(GroupByQueryRunnerFactory.class);

  private static final int MAX_LOCAL_SPLIT = 32;

  private final GroupByQueryEngine engine;
  private final QueryConfig config;

  @Inject
  public GroupByQueryRunnerFactory(
      GroupByQueryEngine engine,
      QueryWatcher queryWatcher,
      QueryConfig config,
      GroupByQueryQueryToolChest toolChest,
      @Global StupidPool<ByteBuffer> computationBufferPool
  )
  {
    super(toolChest, queryWatcher);
    this.engine = engine;
    this.config = config;
  }

  @Override
  public Future<Object> preFactoring(
      GroupByQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    List<ValueType> types = Lists.newArrayList();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      ValueDesc type = dimensionSpec.resolve(resolver.get());
      if (ValueDesc.isMap(type)) {
        String[] descriptiveType = TypeUtils.splitDescriptiveType(type.typeName());
        if (descriptiveType == null) {
          throw new ISE("cannot resolve value type of map %s [%s]", type, dimensionSpec);
        }
        type = ValueDesc.of(descriptiveType[2]);
        if (type.isDimension()) {
          type = ValueDesc.of(ValueDesc.typeOfDimension(type));
        } else if (type.isArray()) {
          type = ValueDesc.elementOfArray(type);
        }
      }
      if (ValueDesc.isDimension(type)) {
        types.add(ValueType.of(type.subElement().typeName()));
      } else if (ValueDesc.isPrimitive(type)) {
        types.add(type.type());
      } else {
        throw new ISE("cannot group-by on non-primitive type %s [%s]", type, dimensionSpec);
      }
    }
    return Futures.<Object>immediateFuture(types);
  }

  @Override
  public List<List<Segment>> splitSegments(
      GroupByQuery query,
      List<Segment> segments,
      Future<Object> optimizer,
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
      AggregatorFactory cardinality = new CardinalityAggregatorFactory(
          "cardinality", null, query.getDimensions(), query.getGroupingSets(), null, true, true
      );
      TimeseriesQuery timeseries = query.asTimeseriesQuery()
                                        .withPostAggregatorSpecs(null)
                                        .withAggregatorSpecs(Arrays.asList(cardinality))
                                        .withGranularity(Granularities.ALL)
                                        .withOverriddenContext(QueryContextKeys.FINALIZE, false);
      List<Segment> current = Lists.newArrayList();
      HyperLogLogCollector prev = null;
      for (int i = 0; i < segments.size(); i++) {
        Segment segment = segments.get(i);
        SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(((Segment.WithDescriptor) segment).getDescriptor());
        Result<TimeseriesResultValue> result = Sequences.only(
            timeseries.withQuerySegmentSpec(segmentSpec)
                      .run(segmentWalker, Maps.<String, Object>newHashMap())
        );
        HyperLogLogCollector collector = (HyperLogLogCollector) result.getValue().getMetric("cardinality");
        if (prev != null) {
          collector = collector.fold(prev);
        }
        if (collector.estimateCardinalityRound() > maxCardinality) {
          if (current.isEmpty()) {
            current.add(segment);
            segmentGroups.add(current);
            current = Lists.newArrayList();
          } else {
            segmentGroups.add(current);
            current = Lists.newArrayList(segment);
          }
          prev = null;
        } else {
          current.add(segment);
          prev = collector;
        }
        if (i > 0 && segmentGroups.isEmpty() &&
            prev.estimateCardinality() / maxCardinality < (double) i * 2 / segments.size()) {
          // early exiting
          return null;
        }
      }
      if (!current.isEmpty()) {
        segmentGroups.add(current);
      }
    } else {
      int segmentSize = segments.size() / numSplit;
      segmentGroups = segmentSize <= 1 ? ImmutableList.<List<Segment>>of() : Lists.partition(segments, segmentSize);
    }
    return segmentGroups.size() <= 1 ? null : segmentGroups;
  }

  @Override
  public Iterable<GroupByQuery> splitQuery(
      GroupByQuery query,
      List<Segment> segments,
      Future<Object> optimizer,
      Supplier<RowResolver> resolver,
      QuerySegmentWalker segmentWalker
  )
  {
    if (!Granularities.ALL.equals(query.getGranularity())) {
      return null;  // cannot split on column
    }
    if (query.getLimitSpec().getSegmentLimit() != null) {
      return null;  // not sure of this
    }
    List<DimensionSpec> dimensionSpecs = query.getDimensions();
    if (dimensionSpecs.isEmpty()) {
      return null;  // use timeseries query
    }
    GroupByQueryConfig gbyConfig = config.getGroupBy();
    int numSplit = query.getContextInt(Query.GBY_LOCAL_SPLIT_NUM, gbyConfig.getLocalSplitNum());
    if (numSplit < 0) {
      int maxCardinality = query.getContextInt(Query.GBY_LOCAL_SPLIT_CARDINALITY, gbyConfig.getLocalSplitCardinality());
      if (maxCardinality > 1) {
        long cardinality = Queries.estimateCardinality(query, segmentWalker, config);
        numSplit = Math.min(MAX_LOCAL_SPLIT, (int) Math.ceil((double) cardinality / maxCardinality));
      }
    }
    if (numSplit < 2) {
      return null;
    }

    // can split on all dimensions but it seemed not cost-effective
    Object[] thresholds = null;
    DimensionSpec dimensionSpec = dimensionSpecs.get(0);

    String strategy = query.getContextValue(Query.LOCAL_SPLIT_STRATEGY, "slopedSpaced");
    List<DictionaryEncodedColumn> dictionaries = Segments.findDictionaryIndexed(segments, dimensionSpec.getDimension());
    if (!dictionaries.isEmpty()) {
      Union union = (Union) SetOperation.builder().setNominalEntries(64).build(Family.UNION);
      for (DictionaryEncodedColumn dictionary : dictionaries) {
        if (dictionary.hasSketch()) {
          union.update(dictionary.getTheta());
        }
      }
      int cardinality = (int) union.getResult().getEstimate();
      if (cardinality > 0) {
        numSplit = Math.max(numSplit, 1 + (cardinality >> 18));
        if (numSplit < 2) {
          return null;
        }
      }
      ItemsUnion<String> itemsUnion = ItemsUnion.getInstance(32, Ordering.natural().nullsFirst());
      for (DictionaryEncodedColumn dictionary : dictionaries) {
        if (dictionary.hasSketch()) {
          itemsUnion.update(dictionary.getQuantile());
        }
      }
      if (!itemsUnion.isEmpty()) {
        thresholds = (Object[]) QuantileOperation.QUANTILES.calculate(
            itemsUnion.getResult(), QuantileOperation.valueOf(strategy, numSplit + 1, true)
        );
      }
    }
    if (thresholds == null) {
      thresholds = Queries.makeColumnHistogramOn(
          resolver, segmentWalker, query.asTimeseriesQuery(), dimensionSpec, numSplit, strategy
      );
    }
    if (thresholds == null || thresholds.length < 3) {
      return null;
    }
    logger.info("split %s on values : %s", dimensionSpec.getDimension(), Arrays.toString(thresholds));

    ValueDesc type = dimensionSpec.resolve(resolver.get());
    if (type.isDimension()) {
      type = ValueDesc.STRING;
    }
    OrderingSpec orderingSpec = OrderingSpec.create(null);
    if (dimensionSpec instanceof DimensionSpecWithOrdering) {
      DimensionSpecWithOrdering explicit = (DimensionSpecWithOrdering) dimensionSpec;
      orderingSpec = explicit.asOrderingSpec();
    }
    Map<String, String> mapping = QueryUtils.aliasMapping(query);
    String dimension = mapping.getOrDefault(dimensionSpec.getOutputName(), dimensionSpec.getOutputName());

    Direction direction = orderingSpec.getDirection();
    List<GroupByQuery> splits = Lists.newArrayList();
    for (int i = 1; i < thresholds.length; i++) {
      BoundDimFilter filter;
      if (i == 1) {
        filter = direction == Direction.ASCENDING ?
                     BoundDimFilter.lt(dimension, thresholds[i]) :
                     BoundDimFilter.gte(dimension, thresholds[i]);
      } else if (i < thresholds.length - 1) {
        filter = direction == Direction.ASCENDING ?
                     BoundDimFilter.between(dimension, thresholds[i - 1], thresholds[i]) :
                     BoundDimFilter.between(dimension, thresholds[i], thresholds[i - 1]);
      } else {
        filter = direction == Direction.ASCENDING ?
                     BoundDimFilter.gte(dimension, thresholds[i - 1]) :
                     BoundDimFilter.lt(dimension, thresholds[i - 1]);
      }
      if (type.isStringOrDimension() && !orderingSpec.isNaturalOrdering()) {
        filter = filter.withComparatorType(orderingSpec.getDimensionOrder());
      }
      logger.debug("--> filter : %s", filter);
      splits.add(
          query.withDimFilter(DimFilters.and(query.getDimFilter(), filter))
      );
    }
    if (query.isDescending()) {
      splits = Lists.reverse(splits);
    }
    return splits;
  }

  @Override
  public QueryRunner<Row> createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new GroupByQueryRunner(segment, engine, cache);
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      final ExecutorService exec,
      final Iterable<QueryRunner<Row>> queryRunners,
      final Future<Object> optimizer
  )
  {
    // mergeRunners should take ListeningExecutorService at some point
    return new GroupByMergedQueryRunner(
        MoreExecutors.listeningDecorator(exec),
        config.getGroupBy(),
        queryWatcher,
        queryRunners
    );
  }

  private static class GroupByQueryRunner implements QueryRunner<Row>
  {
    private final Segment segment;
    private final GroupByQueryEngine engine;
    private final Cache cache;

    public GroupByQueryRunner(Segment segment, GroupByQueryEngine engine, Cache cache)
    {
      this.segment = segment;
      this.engine = engine;
      this.cache = cache;
    }

    @Override
    public Sequence<Row> run(Query<Row> input, Map<String, Object> responseContext)
    {
      if (!(input instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), GroupByQuery.class);
      }

      return engine.process((GroupByQuery) input, segment, true, cache);
    }

    @Override
    public String toString()
    {
      return segment.getIdentifier();
    }
  }
}
