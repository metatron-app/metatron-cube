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
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.StupidPool;
import io.druid.common.guava.CombiningSequence;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.granularity.Granularity;
import io.druid.guice.annotations.Global;
import io.druid.query.BaseQuery;
import io.druid.query.CacheStrategy;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.LateralViewSpec;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.SubqueryQueryRunner;
import io.druid.query.TabularFormat;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.aggregation.model.HoltWintersPostProcessor;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.incremental.IncrementalIndex;
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
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 */
public class GroupByQueryQueryToolChest extends QueryToolChest<Row, GroupByQuery>
{
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>()
  {
  };

  private final Supplier<GroupByQueryConfig> configSupplier;

  private final StupidPool<ByteBuffer> bufferPool;
  private final GroupByQueryEngine engine; // For running the outer query around a subquery

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public GroupByQueryQueryToolChest(
      Supplier<GroupByQueryConfig> configSupplier,
      GroupByQueryEngine engine,
      @Global StupidPool<ByteBuffer> bufferPool,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.configSupplier = configSupplier;
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
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        if (BaseQuery.getContextBySegment(query, false)) {
          return runner.run(query, responseContext);
        }

        if (query.getContextBoolean(QueryContextKeys.FINAL_WORK, true)) {
          return mergeGroupByResults(
              (GroupByQuery) query,
              runner,
              responseContext
          );
        }
        return runner.run(query, responseContext);
      }
    };
  }

  private Sequence<Row> mergeGroupByResults(
      final GroupByQuery query,
      final QueryRunner<Row> runner,
      final Map<String, Object> context
  )
  {
    final Long fudgeTimestamp = GroupByQueryEngine.getUniversalTimestamp(query);
    final GroupByQuery actualQuery = removePostActions(query)
        .withOverriddenContext(
            ImmutableMap.<String, Object>of(
                GroupByQueryHelper.CTX_KEY_FUDGE_TIMESTAMP,
                fudgeTimestamp == null ? "" : String.valueOf(fudgeTimestamp)
            )
        );
    return postProcessing(query, mergeSequence(actualQuery, runner.run(actualQuery, context)));
  }

  private GroupByQuery removePostActions(GroupByQuery query)
  {
    return new GroupByQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        query.getDimFilter(),
        query.getGranularity(),
        query.getDimensions(),
        query.getVirtualColumns(),
        query.getAggregatorSpecs(),
        // Don't do post aggs until the end of this method.
        ImmutableList.<PostAggregator>of(),
        // Don't do "having" clause until the end of this method.
        null,
        null,
        null,
        null,
        query.getContext()
    ).withOverriddenContext(
        ImmutableMap.<String, Object>of(
            QueryContextKeys.FINALIZE, false,
            QueryContextKeys.FINAL_WORK, false
        )
    );
  }

  private CombiningSequence<Row> mergeSequence(GroupByQuery outerQuery, Sequence<Row> outerSequence)
  {
    return CombiningSequence.create(
        outerSequence,
        outerQuery.getRowOrdering(),
        new GroupByBinaryFnV2(outerQuery)
    );
  }

  @Override
  public <I> QueryRunner<Row> handleSubQuery(
      final QueryRunner<I> subQueryRunner,
      final QuerySegmentWalker segmentWalker,
      final ExecutorService executor,
      final int maxRowCount
  )
  {
    return new SubQueryRunner<I>(subQueryRunner, segmentWalker, executor, maxRowCount)
    {
      @Override
      protected Sequence<Row> runOuterQuery(
          Query<Row> query,
          Map<String, Object> context,
          Segment segment
      )
      {
        final Sequence<Row> outerSequence = super.runOuterQuery(query, context, segment);
        final GroupByQuery outerQuery = (GroupByQuery) query;
        final IncrementalIndex<?> outerQueryResultIndex =
            outerSequence.accumulate(
                GroupByQueryHelper.createIncrementalIndex(outerQuery, bufferPool, true, maxRowCount, null),
                GroupByQueryHelper.<Row>newIndexAccumulator()
            );
        close(segment);

        return new ResourceClosingSequence<>(
            postAggregate(outerQuery, outerQueryResultIndex),
            outerQueryResultIndex
        );
      }

      @Override
      protected Function<Interval, Sequence<Row>> function(
          Query<Row> query,
          Map<String, Object> context,
          Segment segment
      )
      {
        final GroupByQuery outerQuery = (GroupByQuery) query;
        final StorageAdapter storageAdapter = segment.asStorageAdapter(true);
        return new Function<Interval, Sequence<Row>>()
        {
          @Override
          public Sequence<Row> apply(Interval interval)
          {
            return engine.process(
                outerQuery.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)),
                storageAdapter
            );
          }
        };
      }
    };
  }

  private Sequence<Row> postProcessing(GroupByQuery query, Sequence<Row> mergedSequence)
  {
    final Granularity granularity = query.getGranularity();
    final List<PostAggregator> postAggregators = PostAggregators.decorate(
        query.getPostAggregatorSpecs(),
        query.getAggregatorSpecs()
    );
    if (!postAggregators.isEmpty() || !granularity.isUTC()) {
      mergedSequence = Sequences.map(
          mergedSequence,
          new Function<Row, Row>()
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
          }
      );
    }
    return query.applyLimit(mergedSequence, query.isSortOnTimeForLimit(configSupplier.get().isSortOnTime()));
  }

  private Sequence<Row> postAggregate(GroupByQuery query, IncrementalIndex<?> index)
  {
    final Granularity granularity = query.getGranularity();
    final List<PostAggregator> postAggregators = query.getPostAggregatorSpecs();  // decorated inside of index
    Iterable<Row> sequence = index.iterableWithPostAggregations(postAggregators, query.isDescending());
    if (!granularity.isUTC()) {
      sequence = Iterables.transform(
          sequence,
          new Function<Row, Row>()
          {
            @Override
            public Row apply(Row input)
            {
              final MapBasedRow row = (MapBasedRow) input;
              return new MapBasedRow(granularity.toDateTime(row.getTimestampFromEpoch()), row.getEvent());
            }
          }
      );
    }
    return query.applyLimit(
        Sequences.simple(sequence),
        query.isSortOnTimeForLimit(configSupplier.get().isSortOnTime())
    );
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(GroupByQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query)
                       .setDimension("numDimensions", String.valueOf(query.getDimensions().size()))
                       .setDimension("numMetrics", String.valueOf(query.getAggregatorSpecs().size()))
                       .setDimension(
                           "numComplexMetrics",
                           String.valueOf(DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs()))
                       );
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
      @Override
      public Row apply(Row input)
      {
        Row.Updatable updatable = Rows.toUpdatable(input);
        for (AggregatorFactory agg : query.getAggregatorSpecs()) {
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
    return new SubqueryQueryRunner<>(
        intervalChunkingQueryRunnerDecorator.decorate(
            new QueryRunner<Row>()
            {
              @Override
              public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
              {
                GroupByQuery groupByQuery = (GroupByQuery) query;
                if (groupByQuery.getDimFilter() != null) {
                  groupByQuery = groupByQuery.withDimFilter(groupByQuery.getDimFilter().optimize());
                }
                final GroupByQuery delegateGroupByQuery = groupByQuery;
                ArrayList<DimensionSpec> dimensionSpecs = new ArrayList<>();
                Set<String> optimizedDimensions = ImmutableSet.copyOf(
                    Iterables.transform(
                        extractionsToRewrite(delegateGroupByQuery),
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
                for (DimensionSpec dimensionSpec : delegateGroupByQuery.getDimensions()) {
                  if (optimizedDimensions.contains(dimensionSpec.getDimension())) {
                    dimensionSpecs.add(
                        new DefaultDimensionSpec(dimensionSpec.getDimension(), dimensionSpec.getOutputName())
                    );
                  } else {
                    dimensionSpecs.add(dimensionSpec);
                  }
                }
                return runner.run(
                    delegateGroupByQuery.withDimensionSpecs(dimensionSpecs),
                    responseContext
                );
              }
            }, this
        )
    );
  }

  @Override
  public CacheStrategy<Row, Object, GroupByQuery> getCacheStrategy(final GroupByQuery query)
  {
    return new CacheStrategy<Row, Object, GroupByQuery>()
    {
      private static final byte CACHE_STRATEGY_VERSION = 0x1;
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();
      private final List<DimensionSpec> dims = query.getDimensions();


      @Override
      public byte[] computeCacheKey(GroupByQuery query)
      {
        final DimFilter dimFilter = query.getDimFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(query.getAggregatorSpecs());
        final byte[] granularityBytes = query.getGranularity().getCacheKey();
        final byte[][] dimensionsBytes = new byte[query.getDimensions().size()][];
        int dimensionsBytesSize = 0;
        int index = 0;
        for (DimensionSpec dimension : query.getDimensions()) {
          dimensionsBytes[index] = dimension.getCacheKey();
          dimensionsBytesSize += dimensionsBytes[index].length;
          ++index;
        }
        final byte[] vcBytes = QueryCacheHelper.computeAggregatorBytes(query.getVirtualColumns());
        final byte[] explodeBytes = QueryCacheHelper.computeCacheBytes(query.getLateralView());
        final byte[] limitBytes = query.getLimitSpec().getCacheKey();
        final byte[] outputColumnsBytes = QueryCacheHelper.computeCacheBytes(query.getOutputColumns());

        ByteBuffer buffer = ByteBuffer
            .allocate(
                2
                + granularityBytes.length
                + filterBytes.length
                + aggregatorBytes.length
                + dimensionsBytesSize
                + vcBytes.length
                + explodeBytes.length
                + limitBytes.length
                + outputColumnsBytes.length
            )
            .put(GROUPBY_QUERY)
            .put(CACHE_STRATEGY_VERSION)
            .put(granularityBytes)
            .put(filterBytes)
            .put(aggregatorBytes);

        for (byte[] dimensionsByte : dimensionsBytes) {
          buffer.put(dimensionsByte);
        }

        return buffer
            .put(vcBytes)
            .put(explodeBytes)
            .put(limitBytes)
            .put(outputColumnsBytes)
            .array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Row, Object> prepareForCache()
      {
        return new Function<Row, Object>()
        {
          @Override
          public Object apply(Row input)
          {
            if (input instanceof MapBasedRow) {
              final MapBasedRow row = (MapBasedRow) input;
              final List<Object> retVal = Lists.newArrayListWithCapacity(1 + dims.size() + aggs.size());
              retVal.add(row.getTimestamp().getMillis());
              Map<String, Object> event = row.getEvent();
              for (DimensionSpec dim : dims) {
                retVal.add(event.get(dim.getOutputName()));
              }
              for (AggregatorFactory agg : aggs) {
                retVal.add(event.get(agg.getName()));
              }
              return retVal;
            }

            throw new ISE("Don't know how to cache input rows of type[%s]", input.getClass());
          }
        };
      }

      @Override
      public Function<Object, Row> pullFromCache()
      {
        return new Function<Object, Row>()
        {
          private final Granularity granularity = query.getGranularity();

          @Override
          public Row apply(Object input)
          {
            Iterator<Object> results = ((List<Object>) input).iterator();

            DateTime timestamp = granularity.toDateTime(((Number) results.next()).longValue());

            Map<String, Object> event = Maps.newLinkedHashMap();
            Iterator<DimensionSpec> dimsIter = dims.iterator();
            while (dimsIter.hasNext() && results.hasNext()) {
              final DimensionSpec factory = dimsIter.next();
              event.put(factory.getOutputName(), results.next());
            }

            Iterator<AggregatorFactory> aggsIter = aggs.iterator();
            while (aggsIter.hasNext() && results.hasNext()) {
              final AggregatorFactory factory = aggsIter.next();
              event.put(factory.getName(), factory.deserialize(results.next()));
            }

            if (dimsIter.hasNext() || aggsIter.hasNext() || results.hasNext()) {
              throw new ISE(
                  "Found left over objects while reading from cache!! dimsIter[%s] aggsIter[%s] results[%s]",
                  dimsIter.hasNext(),
                  aggsIter.hasNext(),
                  results.hasNext()
              );
            }

            return new MapBasedRow(
                timestamp,
                event
            );
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
  public TabularFormat toTabularFormat(final Sequence<Row> sequence, final String timestampColumn)
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
    final List<String> outputColumns = ((GroupByQuery) query).getOutputColumns();
    final LateralViewSpec lateralViewSpec = ((GroupByQuery) query).getLateralView();
    if (outputColumns != null) {
      sequence = Sequences.map(
          sequence, new Function<Row, Row>()
          {
            @Override
            public Row apply(Row input)
            {
              if (input instanceof HoltWintersPostProcessor.PredictedRow) {
                return input;   // nasty
              }
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
