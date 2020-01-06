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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.druid.data.input.Row;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.collections.StupidPool;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Rows;
import io.druid.guice.annotations.Global;
import io.druid.query.BaseAggregationQueryToolChest;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class GroupByQueryQueryToolChest extends BaseAggregationQueryToolChest<GroupByQuery>
{
  private final QueryConfig config;

  private final StupidPool<ByteBuffer> bufferPool;
  private final GroupByQueryEngine engine; // For running the outer query around a subquery
  private final GroupByQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public GroupByQueryQueryToolChest(
      QueryConfig config,
      GroupByQueryEngine engine,
      @Global StupidPool<ByteBuffer> bufferPool,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this(config, engine, bufferPool, intervalChunkingQueryRunnerDecorator, DefaultGroupByQueryMetricsFactory.instance());
  }

  @Inject
  public GroupByQueryQueryToolChest(
      QueryConfig config,
      GroupByQueryEngine engine,
      @Global StupidPool<ByteBuffer> bufferPool,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator,
      GroupByQueryMetricsFactory queryMetricsFactory
  )
  {
    super(intervalChunkingQueryRunnerDecorator);
    this.config = config;
    this.engine = engine;
    this.bufferPool = bufferPool;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  protected byte queryCode()
  {
    return GROUPBY_QUERY;
  }

  @Override
  protected Ordering<Row> getMergeOrdering(final GroupByQuery groupBy)
  {
    return Ordering.from(
        new Comparator<Row>()
        {
          private final Comparator[] comparators = DimensionSpecs.toComparator(groupBy.getDimensions(), true);

          @Override
          @SuppressWarnings("unchecked")
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

  @Override
  public <I> QueryRunner<Row> handleSubQuery(QuerySegmentWalker segmentWalker, QueryConfig config)
  {
    return new SubQueryRunner<I>(segmentWalker, config)
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        final GroupByQuery groupBy = (GroupByQuery) query;
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
            GroupByQueryHelper.createMergeIndex(groupBy, config, 1),
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
            }.asArray();

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

  @Override
  public GroupByQueryMetrics makeMetrics(GroupByQuery query)
  {
    GroupByQueryMetrics queryMetrics = queryMetricsFactory.makeMetrics();
    queryMetrics.query(query);
    return queryMetrics;
  }

  @Override
  public Function<Row, Row> makePostComputeManipulatorFn(final GroupByQuery query, final MetricManipulationFn fn)
  {
    final Function<Row, Row> postCompute = super.makePostComputeManipulatorFn(query, fn);
    final Set<String> optimizedDims = ImmutableSet.copyOf(
        DimensionSpecs.toOutputNames(extractionsToRewrite(query))
    );
    if (optimizedDims.isEmpty()) {
      return postCompute;
    }

    // If we have optimizations that can be done at this level, we apply them here
    final Map<String, ExtractionFn> extractionFnMap = new HashMap<>();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      final String dimension = dimensionSpec.getOutputName();
      if (optimizedDims.contains(dimension)) {
        extractionFnMap.put(dimension, dimensionSpec.getExtractionFn());
      }
    }

    return new Function<Row, Row>()
    {
      @Override
      public Row apply(Row input)
      {
        final Row.Updatable updatable = Rows.toUpdatable(postCompute.apply(input));
        for (String dim : optimizedDims) {
          final Object eventVal = updatable.getRaw(dim);
          updatable.set(dim, extractionFnMap.get(dim).apply(eventVal));
        }
        return updatable;
      }
    };
  }

  @Override
  public QueryRunner<Row> preMergeQueryDecoration(final QueryRunner<Row> runner)
  {
    return super.preMergeQueryDecoration(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
          {
            GroupByQuery groupBy = (GroupByQuery) query;
            List<DimensionSpec> dimensionSpecs = Lists.newArrayList();
            Set<String> optimizedDimensions = ImmutableSet.copyOf(
                DimensionSpecs.toInputNames(extractionsToRewrite(groupBy))
            );
            if (!optimizedDimensions.isEmpty()) {
              for (DimensionSpec dimensionSpec : groupBy.getDimensions()) {
                if (optimizedDimensions.contains(dimensionSpec.getDimension())) {
                  dimensionSpec = DefaultDimensionSpec.of(dimensionSpec.getDimension(), dimensionSpec.getOutputName());
                }
                dimensionSpecs.add(dimensionSpec);
              }
              groupBy = groupBy.withDimensionSpecs(dimensionSpecs);
            }
            return runner.run(groupBy, responseContext);
          }
        }
    );
  }

  /**
   * This function checks the query for dimensions which can be optimized by applying the dimension extraction
   * as the final step of the query instead of on every event.
   *
   * @param query The query to check for optimizations
   *
   * @return A collection of DimensionsSpec which can be extracted at the last second upon query completion.
   */
  private Collection<DimensionSpec> extractionsToRewrite(GroupByQuery query)
  {
    return Collections2.filter(
        query.getDimensions(), new Predicate<DimensionSpec>()
        {
          @Override
          public boolean apply(DimensionSpec input)
          {
            return DimensionSpecs.isOneToOneExtraction(input);
          }
        }
    );
  }

  @Override
  protected boolean isSortOnTime()
  {
    return config.getGroupBy().isSortOnTime();
  }
}
