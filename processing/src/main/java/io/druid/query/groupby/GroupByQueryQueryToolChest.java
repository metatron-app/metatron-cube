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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.StupidPool;
import io.druid.common.guava.CombiningSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.granularity.Granularity;
import io.druid.guice.annotations.Global;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.CacheStrategy;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.LateralViewSpec;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryConfig;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryUtils;
import io.druid.query.TabularFormat;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class GroupByQueryQueryToolChest extends QueryToolChest<Row, GroupByQuery>
{
  private static final TypeReference<Object[]> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object[]>()
      {
      };
  private static final TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>()
  {
  };

  private final QueryConfig config;

  private final StupidPool<ByteBuffer> bufferPool;
  private final GroupByQueryEngine engine; // For running the outer query around a subquery

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public GroupByQueryQueryToolChest(
      QueryConfig config,
      GroupByQueryEngine engine,
      @Global StupidPool<ByteBuffer> bufferPool,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.config = config;
    this.engine = engine;
    this.bufferPool = bufferPool;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Row> mergeResults(final QueryRunner<Row> runner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        GroupByQuery groupBy = (GroupByQuery) query;
        if (groupBy.getContextBoolean(QueryContextKeys.FINAL_MERGE, true)) {
          Sequence<Row> sequence = runner.run(groupBy.removePostActions(), responseContext);
          if (BaseQuery.getContextBySegment(groupBy)) {
            return Sequences.map((Sequence) sequence, BySegmentResultValueClass.applyAll(
                Functions.compose(toPostAggregator(groupBy), toMapBasedRow(groupBy)))
            );
          }
          sequence = CombiningSequence.create(sequence, groupBy.getRowOrdering(), new GroupByBinaryFnV2(groupBy));
          sequence = Sequences.map(sequence, Functions.compose(toPostAggregator(groupBy), toMapBasedRow(groupBy)));
          return sequence;
        }
        return runner.run(groupBy, responseContext);
      }
    };
  }

  private Function<Row, Row> toMapBasedRow(final GroupByQuery query)
  {
    return new Function<Row, Row>()
    {
      private final Granularity granularity = query.getGranularity();
      private final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
      private final List<String> metrics = AggregatorFactory.toNames(query.getAggregatorSpecs());

      @Override
      public Row apply(Row input)
      {
        final Object[] values = ((CompactRow) input).getValues();
        final Map<String, Object> event = Maps.newLinkedHashMap();
        int x = 1;
        for (String dimension : dimensions) {
          event.put(dimension, values[x++]);
        }
        for (String metric : metrics) {
          event.put(metric, values[x++]);
        }
        return new MapBasedRow(granularity.toDateTime(input.getTimestampFromEpoch()), event);
      }
    };
  }

  @Override
  public <I> QueryRunner<Row> handleSubQuery(QuerySegmentWalker segmentWalker, int maxRowCount)
  {
    return new SubQueryRunner<I>(segmentWalker, maxRowCount)
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        final GroupByQuery groupBy = (GroupByQuery) query.withOverriddenContext(
            GroupByQueryHelper.CTX_KEY_FUDGE_TIMESTAMP,
            Objects.toString(GroupByQueryEngine.getUniversalTimestamp(query), "")
        );
        final int maxPage = groupBy.getContextIntWithMax(
            Query.GBY_MAX_STREAM_SUBQUERY_PAGE,
            config.getGroupBy().getMaxStreamSubQueryPage()
        );
        final Query<I> innerQuery = ((QueryDataSource) groupBy.getDataSource()).getQuery();
        if (maxPage < 1 || !QueryUtils.coveredBy(innerQuery, groupBy)) {
          return super.run(groupBy, responseContext);
        }
        // this is about using less heap, not about performance
        return runStreaming(groupBy, responseContext);
      }

      @Override
      protected Function<Interval, Sequence<Row>> query(Query<Row> query, final Segment segment)
      {
        final GroupByQuery groupBy = ((GroupByQuery) query).withPostAggregatorSpecs(null);
        return new Function<Interval, Sequence<Row>>()
        {
          @Override
          public Sequence<Row> apply(Interval interval)
          {
            QuerySegmentSpec segmentSpec = MultipleIntervalSegmentSpec.of(interval);
            return engine.process(groupBy.withQuerySegmentSpec(segmentSpec), segment, true);
          }
        };
      }

      @Override
      protected Sequence<Row> mergeQuery(Query<Row> query, Sequence<Sequence<Row>> sequences, Segment segment)
      {
        GroupByQuery groupBy = (GroupByQuery) query;
        Sequence<Row> sequence = super.mergeQuery(query, sequences, segment);
        MergeIndex mergeIndex = sequence.accumulate(
            GroupByQueryHelper.createMergeIndex(groupBy, config.getMaxResults(), 1),
            GroupByQueryHelper.<Row>newMergeAccumulator(new Execs.Semaphore(1))
        );
        sequence = Sequences.withBaggage(mergeIndex.toMergeStream(true), mergeIndex);
        return Sequences.map(
            sequence, Functions.compose(toPostAggregator(groupBy), toMapBasedRow(groupBy))
        );
      }

      @Override
      protected Function<Cursor, Sequence<Row>> streamQuery(final Query<Row> query)
      {
        final GroupByQuery groupBy = (GroupByQuery) query;
        final int maxPages = groupBy.getContextIntWithMax(
            Query.GBY_MAX_STREAM_SUBQUERY_PAGE,
            config.getGroupBy().getMaxStreamSubQueryPage()
        );
        return new Function<Cursor, Sequence<Row>>()
        {
          @Override
          public Sequence<Row> apply(Cursor cursor)
          {
            Iterator<Object[]> iterator = new GroupByQueryEngine.RowIterator(groupBy, cursor, bufferPool, maxPages)
            {
              @Override
              protected void nextIteration(long start, List<int[]> unprocessedKeys)
              {
                if (unprocessedKeys != null) {
                  // todo: fall back to incremanl index?
                  throw new ISE("Result of subquery is exceeding max pages [%d]", maxPages);
                }
              }
            };
            LOG.info("Running streaming subquery with max pages [%d]", maxPages);
            return Sequences.map(
                Sequences.once(iterator),
                Functions.compose(
                    toPostAggregator(groupBy),
                    GroupByQueryEngine.arrayToRow(groupBy.withPostAggregatorSpecs(null), false)
                )
            );
          }
        };
      }
    };
  }

  private Function<Row, Row> toPostAggregator(GroupByQuery query)
  {
    final Granularity granularity = query.getGranularity();
    final List<PostAggregator> postAggregators = PostAggregators.decorate(
        query.getPostAggregatorSpecs(),
        query.getAggregatorSpecs()
    );
    if (postAggregators.isEmpty() && granularity.isUTC()) {
      return Functions.identity();
    }
    return new Function<Row, Row>()
    {
      @Override
      public Row apply(final Row row)
      {
        final Map<String, Object> newMap = Maps.newLinkedHashMap(((MapBasedRow) row).getEvent());

        for (PostAggregator postAggregator : postAggregators) {
          newMap.put(postAggregator.getName(), postAggregator.compute(row.getTimestamp(), newMap));
        }
        return new MapBasedRow(granularity.toDateTime(row.getTimestampFromEpoch()), newMap);
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(GroupByQuery query)
  {
    final List<DimensionSpec> dimensions = query.getDimensions();
    final List<AggregatorFactory> aggregators = query.getAggregatorSpecs();
    return super.makeMetricBuilder(query)
                .setDimension("numDimensions", String.valueOf(dimensions.size()))
                .setDimension("numMetrics", String.valueOf(aggregators.size()))
                .setDimension("numComplexMetrics", String.valueOf(DruidMetrics.findNumComplexAggs(aggregators)));
  }

  @Override
  public Function<Row, Row> makePreComputeManipulatorFn(
      final GroupByQuery query,
      final MetricManipulationFn fn
  )
  {
    if (fn == MetricManipulatorFns.identity()) {
      return Functions.identity();
    }
    return new Function<Row, Row>()
    {
      private final int start = query.getDimensions().size() + 1;
      private final List<AggregatorFactory> metrics = query.getAggregatorSpecs();

      @Override
      public Row apply(Row input)
      {
        if (input instanceof CompactRow) {
          final Object[] values = ((CompactRow) input).getValues();
          int x = start;
          for (AggregatorFactory metric : metrics) {
            values[x] = fn.manipulate(metric, values[x++]);
          }
          return input;
        }
        final Row.Updatable updatable = Rows.toUpdatable(input);
        for (AggregatorFactory agg : metrics) {
          final String name = agg.getName();
          updatable.set(name, fn.manipulate(agg, input.getRaw(name)));
        }
        return updatable;
      }
    };
  }

  @Override
  public Function<Row, Row> makePostComputeManipulatorFn(
      final GroupByQuery query,
      final MetricManipulationFn fn
  )
  {
    final Set<String> optimizedDims = ImmutableSet.copyOf(
        Iterables.transform(
            extractionsToRewrite(query),
            new Function<DimensionSpec, String>()
            {
              @Override
              public String apply(DimensionSpec input)
              {
                return input.getOutputName();
              }
            }
        )
    );
    final Function<Row, Row> preCompute = makePreComputeManipulatorFn(query, fn);
    if (optimizedDims.isEmpty()) {
      return preCompute;
    }

    // If we have optimizations that can be done at this level, we apply them here

    final Map<String, ExtractionFn> extractionFnMap = new HashMap<>();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      final String dimension = dimensionSpec.getOutputName();
      if (optimizedDims.contains(dimension)) {
        extractionFnMap.put(dimension, dimensionSpec.getExtractionFn());
      }
    }

    // cannot inplace update (see GroupByQueryRunnerTest#testBySegmentResultsWithAllFiltersWithExtractionFns)
    return new Function<Row, Row>()
    {
      @Nullable
      @Override
      public Row apply(Row input)
      {
        Row preRow = preCompute.apply(input);
        if (preRow instanceof MapBasedRow) {
          MapBasedRow preMapRow = (MapBasedRow) preRow;
          Map<String, Object> event = Maps.newHashMap(preMapRow.getEvent());
          for (String dim : optimizedDims) {
            final Object eventVal = event.get(dim);
            event.put(dim, extractionFnMap.get(dim).apply(eventVal));
          }
          return new MapBasedRow(preMapRow.getTimestamp(), event);
        } else {
          return preRow;
        }
      }
    };
  }

  @Override
  public TypeReference<Row> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public QueryRunner<Row> preMergeQueryDecoration(final QueryRunner<Row> runner)
  {
    return
        intervalChunkingQueryRunnerDecorator.decorate(
            new QueryRunner<Row>()
            {
              @Override
              public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
              {
                GroupByQuery groupByQuery = (GroupByQuery) query;
                ArrayList<DimensionSpec> dimensionSpecs = new ArrayList<>();
                Set<String> optimizedDimensions = ImmutableSet.copyOf(
                    Iterables.transform(
                        extractionsToRewrite(groupByQuery),
                        new Function<DimensionSpec, String>()
                        {
                          @Override
                          public String apply(DimensionSpec input)
                          {
                            return input.getDimension();
                          }
                        }
                    )
                );
                for (DimensionSpec dimensionSpec : groupByQuery.getDimensions()) {
                  if (optimizedDimensions.contains(dimensionSpec.getDimension())) {
                    dimensionSpecs.add(
                        new DefaultDimensionSpec(dimensionSpec.getDimension(), dimensionSpec.getOutputName())
                    );
                  } else {
                    dimensionSpecs.add(dimensionSpec);
                  }
                }
                return runner.run(
                    groupByQuery.withDimensionSpecs(dimensionSpecs),
                    responseContext
                );
              }
            }, this
    );
  }

  @Override
  @SuppressWarnings("unchecked")
  public CacheStrategy<Row, Object[], GroupByQuery> getCacheStrategy(final GroupByQuery query)
  {
    return new CacheStrategy<Row, Object[], GroupByQuery>()
    {
      private static final byte CACHE_STRATEGY_VERSION = 0x1;
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();
      private final List<DimensionSpec> dims = query.getDimensions();


      @Override
      public byte[] computeCacheKey(GroupByQuery query)
      {
        final byte[] granularityBytes = QueryCacheHelper.computeCacheBytes(query.getGranularity());
        final byte[] filterBytes = QueryCacheHelper.computeCacheBytes(query.getDimFilter());
        final byte[] vcBytes = QueryCacheHelper.computeCacheKeys(query.getVirtualColumns());
        final byte[] dimensionsBytes = QueryCacheHelper.computeCacheKey(query.getDimensions());
        final byte[] aggregatorBytes = QueryCacheHelper.computeCacheKeys(query.getAggregatorSpecs());

        return ByteBuffer
            .allocate(
                2
                + granularityBytes.length
                + filterBytes.length
                + vcBytes.length
                + dimensionsBytes.length
                + aggregatorBytes.length
            )
            .put(GROUPBY_QUERY)
            .put(CACHE_STRATEGY_VERSION)
            .put(granularityBytes)
            .put(filterBytes)
            .put(vcBytes)
            .put(dimensionsBytes)
            .put(aggregatorBytes)
            .array();
      }

      @Override
      public TypeReference<Object[]> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Row, Object[]> prepareForCache()
      {
        return new Function<Row, Object[]>()
        {
          private final List<String> dimensions = DimensionSpecs.toOutputNames(dims);

          @Override
          public Object[] apply(Row input)
          {
            if (input instanceof CompactRow) {
              return ((CompactRow) input).getValues();
            }
            if (input instanceof MapBasedRow) {
              final MapBasedRow row = (MapBasedRow) input;
              final Map<String, Object> event = row.getEvent();

              Object[] values = new Object[1 + dims.size() + aggs.size()];
              int x = 0;
              values[x++] = row.getTimestampFromEpoch();
              for (String dimension : dimensions) {
                values[x++] = event.get(dimension);
              }
              for (AggregatorFactory agg : aggs) {
                values[x++] = event.get(agg.getName());
              }
              return values;
            }

            throw new ISE("Don't know how to cache input rows of type[%s]", input.getClass());
          }
        };
      }

      @Override
      public Function<Object[], Row> pullFromCache()
      {
        return new Function<Object[], Row>()
        {
          private final List<String> dimensions = DimensionSpecs.toOutputNames(dims);
          private final Granularity granularity = query.getGranularity();

          @Override
          public Row apply(final Object[] input)
          {
            if (input.length != 1 + dimensions.size() + aggs.size()) {
              throw new ISE("invalid cached object (length mismatch)");
            }
            int x = 0;
            DateTime timestamp = granularity.toDateTime(((Number) input[x++]).longValue());

            Map<String, Object> event = Maps.newLinkedHashMap();
            for (String dimension : dimensions) {
              event.put(dimension, input[x++]);
            }
            for (final AggregatorFactory metric : aggs) {
              event.put(metric.getName(), metric.deserialize(input[x++]));
            }
            return new MapBasedRow(timestamp, event);
          }
        };
      }
    };
  }


  /**
   * This function checks the query for dimensions which can be optimized by applying the dimension extraction
   * as the final step of the query instead of on every event.
   *
   * @param query The query to check for optimizations
   *
   * @return A collection of DimensionsSpec which can be extracted at the last second upon query completion.
   */
  public static Collection<DimensionSpec> extractionsToRewrite(GroupByQuery query)
  {
    return Collections2.filter(
        query.getDimensions(), new Predicate<DimensionSpec>()
        {
          @Override
          public boolean apply(DimensionSpec input)
          {
            return input.getExtractionFn() != null
                   && ExtractionFn.ExtractionType.ONE_TO_ONE.equals(
                input.getExtractionFn().getExtractionType()
            );
          }
        }
    );
  }

  @Override
  public TabularFormat toTabularFormat(
      final GroupByQuery query,
      final Sequence<Row> sequence,
      final String timestampColumn
  )
  {
    return new TabularFormat()
    {
      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return Sequences.map(
            sequence, new Function<Row, Map<String, Object>>()
            {
              @Override
              public Map<String, Object> apply(Row input)
              {
                Map<String, Object> event = ((MapBasedRow) input).getEvent();
                if (timestampColumn != null) {
                  if (!MapBasedRow.supportInplaceUpdate(event)) {
                    event = Maps.newLinkedHashMap(event);
                  }
                  event.put(timestampColumn, input.getTimestamp());
                }
                return event;
              }
            }
        );
      }

      @Override
      public Map<String, Object> getMetaData()
      {
        return null;
      }
    };
  }

  @Override
  public QueryRunner<Row> finalQueryDecoration(final QueryRunner<Row> runner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        return finalDecoration(query, runner.run(query, responseContext));
      }
    };
  }

  private Sequence<Row> finalDecoration(Query<Row> query, Sequence<Row> sequence)
  {
    GroupByQuery groupBy = (GroupByQuery) query;
    sequence = groupBy.applyLimit(sequence, groupBy.isSortOnTimeForLimit(config.getGroupBy().isSortOnTime()));

    final List<String> outputColumns = groupBy.getOutputColumns();
    final LateralViewSpec lateralViewSpec = groupBy.getLateralView();
    if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
      sequence = Sequences.map(
          sequence, new Function<Row, Row>()
          {
            @Override
            public Row apply(Row input)
            {
              DateTime timestamp = input.getTimestamp();
              Map<String, Object> retained = Maps.newHashMapWithExpectedSize(outputColumns.size());
              for (String retain : outputColumns) {
                retained.put(retain, input.getRaw(retain));
              }
              return new MapBasedRow(timestamp, retained);
            }
          }
      );
    }
    return lateralViewSpec != null ? toLateralView(sequence, lateralViewSpec) : sequence;
  }

  Sequence<Row> toLateralView(Sequence<Row> result, final LateralViewSpec lateralViewSpec)
  {
    return Sequences.concat(
        Sequences.map(
            result, new Function<Row, Sequence<Row>>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Sequence<Row> apply(Row input)
              {
                final DateTime timestamp = input.getTimestamp();
                final Map<String, Object> event = ((MapBasedRow) input).getEvent();
                return Sequences.simple(
                    Iterables.transform(
                        lateralViewSpec.apply(event),
                        new Function<Map<String, Object>, Row>()
                        {
                          @Override
                          public Row apply(Map<String, Object> input)
                          {
                            return new MapBasedRow(timestamp, input);
                          }
                        }
                    )
                );
              }
            }
        )
    );
  }
}
