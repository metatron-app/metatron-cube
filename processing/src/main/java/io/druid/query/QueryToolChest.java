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

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.data.input.Row;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.select.Schema;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.Cursor;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.StorageAdapter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.LogicalSegment;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * The broker-side (also used by server in some cases) API for a specific Query type.  This API is still undergoing
 * evolution and is only semi-stable, so proprietary Query implementations should be ready for the potential
 * maintenance burden when upgrading versions.
 */
public abstract class QueryToolChest<ResultType, QueryType extends Query<ResultType>>
{
  protected final Logger LOG = new Logger(getClass());

  // cacheable queries
  protected static final byte TIMESERIES_QUERY = 0x0;
  protected static final byte TOPN_QUERY = 0x1;
  protected static final byte TIMEBOUNDARY_QUERY = 0x3;
  protected static final byte SEGMENT_METADATA_QUERY = 0x4;
  protected static final byte SELECT_QUERY = 0x13;
  protected static final byte GROUPBY_QUERY = 0x14;
  protected static final byte SEARCH_QUERY = 0x15;
  protected static final byte SKETCH_QUERY = 0x16;
  protected static final byte SELECT_META_QUERY = 0x17;

  /**
   * This method wraps a QueryRunner.  The input QueryRunner, by contract, will provide a series of
   * ResultType objects in time order (ascending or descending).  This method should return a new QueryRunner that
   * potentially merges the stream of ordered ResultType objects.
   *
   * @param runner A QueryRunner that provides a series of ResultType objects in time order (ascending or descending)
   *
   * @return a QueryRunner that potentially merges the stream of ordered ResultType objects
   */
  public abstract QueryRunner<ResultType> mergeResults(QueryRunner<ResultType> runner);

  /**
   * Creates a builder that is used to generate a metric for this specific query type.  This exists
   * to allow for query-specific dimensions on metrics.  That is, the ToolChest is expected to set some
   * meaningful dimensions for metrics given this query type.  Examples might be the topN threshold for
   * a TopN query or the number of dimensions included for a groupBy query.
   *
   * @param query The query that is being processed
   *
   * @return A MetricEvent.Builder that can be used to make metrics for the provided query
   */
  public ServiceMetricEvent.Builder makeMetricBuilder(QueryType query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  /**
   * Creates a Function that can take in a ResultType and return a new ResultType having applied
   * the MetricManipulatorFn to each of the metrics.
   * <p>
   * This exists because the QueryToolChest is the only thing that understands the internal serialization
   * format of ResultType, so it's primary responsibility is to "decompose" that structure and apply the
   * given function to all metrics.
   * <p>
   * This function is called very early in the processing pipeline on the Broker.
   *
   * @param query The Query that is currently being processed
   * @param fn    The function that should be applied to all metrics in the results
   *
   * @return A function that will apply the provided fn to all metrics in the input ResultType object
   */
  public Function<ResultType, ResultType> makePreComputeManipulatorFn(QueryType query, MetricManipulationFn fn)
  {
    return Functions.identity();
  }

  /**
   * Generally speaking this is the exact same thing as makePreComputeManipulatorFn.  It is leveraged in
   * order to compute PostAggregators on results after they have been completely merged together, which
   * should actually be done in the mergeResults() call instead of here.
   * <p>
   * This should never actually be overridden and it should be removed as quickly as possible.
   *
   * @param query The Query that is currently being processed
   * @param fn    The function that should be applied to all metrics in the results
   *
   * @return A function that will apply the provided fn to all metrics in the input ResultType object
   */
  public Function<ResultType, ResultType> makePostComputeManipulatorFn(QueryType query, MetricManipulationFn fn)
  {
    return Functions.identity();
  }

  /**
   * Returns a TypeReference object that is just passed through to Jackson in order to deserialize
   * the results of this type of query.
   *
   * @return A TypeReference to indicate to Jackson what type of data will exist for this query
   */
  public abstract TypeReference<ResultType> getResultTypeReference();

  /**
   * Returns a CacheStrategy to be used to load data into the cache and remove it from the cache.
   * <p>
   * This is optional.  If it returns null, caching is effectively disabled for the query.
   *
   * @param query The query whose results might be cached
   * @param <T>   The type of object that will be stored in the cache
   *
   * @return A CacheStrategy that can be used to populate and read from the Cache
   */
  public <T> CacheStrategy<ResultType, T, QueryType> getCacheStrategy(QueryType query)
  {
    return null;
  }

  /**
   * Wraps a QueryRunner.  The input QueryRunner is the QueryRunner as it exists *before* being passed to
   * mergeResults().
   * <p>
   * In fact, the return value of this method is always passed to mergeResults, so it is equivalent to
   * just implement this functionality as extra decoration on the QueryRunner during mergeResults().
   * <p>
   * In the interests of potentially simplifying these interfaces, the recommendation is to actually not
   * override this method and instead apply anything that might be needed here in the mergeResults() call.
   *
   * @param runner The runner to be wrapped
   *
   * @return The wrapped runner
   */
  public QueryRunner<ResultType> preMergeQueryDecoration(QueryRunner<ResultType> runner)
  {
    return runner;
  }

  /**
   * Wraps a QueryRunner.  The input QueryRunner is the QueryRunner as it exists coming out of mergeResults()
   * <p>
   * In fact, the input value of this method is always the return value from mergeResults, so it is equivalent
   * to just implement this functionality as extra decoration on the QueryRunner during mergeResults().
   * <p>
   * In the interests of potentially simplifying these interfaces, the recommendation is to actually not
   * override this method and instead apply anything that might be needed here in the mergeResults() call.
   *
   * @param runner The runner to be wrapped
   *
   * @return The wrapped runner
   */
  public QueryRunner<ResultType> postMergeQueryDecoration(QueryRunner<ResultType> runner)
  {
    return runner;
  }

  @SuppressWarnings("unchecked")
  public QueryRunner<ResultType> finalizeMetrics(QueryRunner<ResultType> runner)
  {
    return new FinalizeResultsQueryRunner<>(runner, (QueryToolChest<ResultType, Query<ResultType>>) this);
  }

  public QueryRunner<ResultType> finalQueryDecoration(QueryRunner<ResultType> runner)
  {
    return runner;
  }

  /**
   * This method is called to allow the query to prune segments that it does not believe need to actually
   * be queried.  It can use whatever criteria it wants in order to do the pruning, it just needs to
   * return the list of Segments it actually wants to see queried.
   *
   * @param query    The query being processed
   * @param segments The list of candidate segments to be queried
   * @param <T>      A Generic parameter because Java is cool
   *
   * @return The list of segments to actually query
   */
  public <T extends LogicalSegment> List<T> filterSegments(QueryType query, List<T> segments)
  {
    return segments;
  }

  public QueryType optimizeQuery(QueryType query, QuerySegmentWalker walker)
  {
    return query;
  }

  /**
   * converts result to tabular format to be stored file system.
   * currently, only select and group-by query supports this.
   *
   * @param query
   * @param sequence
   * @return
   */
  public TabularFormat toTabularFormat(QueryType query, Sequence<ResultType> sequence, String timestampColumn)
  {
    throw new UnsupportedOperationException("toTabularFormat");
  }

  @SuppressWarnings("unchecked")
  public <I> Query<I> prepareSubQuery(QueryType outerQuery, Query<I> innerQuery)
  {
    innerQuery = innerQuery.withOverriddenContext(BaseQuery.copyContextForMeta(outerQuery.getContext()));
    if (innerQuery instanceof Query.DimFilterSupport) {
      // todo pushdown predicate if possible (need input schema)
    }
    return innerQuery;
  }

  /**
   * @param subQueryRunner
   * @param segmentWalker
   * @param executor
   * @param maxRowCount
   */
  public <I> QueryRunner<ResultType> handleSubQuery(
      QueryRunner<I> subQueryRunner,
      QuerySegmentWalker segmentWalker,
      ExecutorService executor,
      int maxRowCount
  )
  {
    throw new UnsupportedOperationException("handleSourceQuery");
  }

  public Sequence<ResultType> mergeSequences(QueryType query, List<Sequence<ResultType>> sequences)
  {
    return QueryUtils.mergeSort(query, sequences);
  }

  protected abstract class SubQueryRunner<I> implements QueryRunner<ResultType>
  {
    protected final QueryRunner<I> subQueryRunner;
    protected final QuerySegmentWalker segmentWalker;
    protected final ExecutorService executor;
    protected final int maxRowCount;

    protected SubQueryRunner(
        QueryRunner<I> subQueryRunner,
        QuerySegmentWalker segmentWalker,
        ExecutorService executor,
        int maxRowCount
    )
    {
      this.subQueryRunner = subQueryRunner;
      this.segmentWalker = segmentWalker;
      this.executor = executor;
      this.maxRowCount = maxRowCount;
    }

    @Override
    public Sequence<ResultType> run(Query<ResultType> query, Map<String, Object> responseContext)
    {
      IncrementalIndex accumulated = accumulate(query, responseContext);
      if (accumulated.isEmpty()) {
        return Sequences.empty();
      }
      query = QueryUtils.resolveQuery(query, segmentWalker);

      List<String> dataSources = query.getDataSource().getNames();
      query = query.withDataSource(TableDataSource.of(StringUtils.join(dataSources, '_')));

      String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());
      StorageAdapter adapter = new IncrementalIndexStorageAdapter.Temporary(dataSource, accumulated);
      Segment segment = new IncrementalIndexSegment(accumulated, adapter.getSegmentIdentifier());

      Supplier<RowResolver> resolver = RowResolver.supplier(segment, query);
      return runOuterQuery(query.resolveQuery(resolver), responseContext, Segments.attach(segment, resolver));
    }

    @SuppressWarnings("unchecked")
    protected IncrementalIndex accumulate(Query<ResultType> query, Map<String, Object> responseContext)
    {
      long start = System.currentTimeMillis();
      QueryDataSource dataSource = (QueryDataSource) query.getDataSource();

      Query<I> subQuery = dataSource.getQuery();
      Sequence<Row> innerSequence = Queries.convertToRow(subQuery, subQueryRunner.run(subQuery, responseContext));
      IncrementalIndexSchema schema = Queries.relaySchema(subQuery, segmentWalker);
      LOG.info(
          "Accumulating into intermediate index with dimensions %s and metrics %s",
          schema.getDimensionsSpec().getDimensionNameTypes(),
          schema.getMetricNameTypes()
      );
      IncrementalIndex index = new OnheapIncrementalIndex(schema, false, true, true, false, maxRowCount);
      IncrementalIndex accumulated = innerSequence.accumulate(index, GroupByQueryHelper.<Row>newIndexAccumulator());
      LOG.info(
          "Accumulated sub-query into index in %,d msec.. total %,d rows",
          (System.currentTimeMillis() - start),
          accumulated.size()
      );
      dataSource.setSchema(schema.asSchema(false));   // will be used to resolve schema of outer query
      return accumulated;
    }

    protected Sequence<ResultType> runOuterQuery(Query<ResultType> query, Map<String, Object> context, Segment segment)
    {
      final Function<Interval, Sequence<ResultType>> function = function(query, context, segment);
      return new ResourceClosingSequence<>(
          Sequences.concat(Sequences.map(Sequences.simple(query.getIntervals()), function)), segment
      );
    }

    protected abstract Function<Interval, Sequence<ResultType>> function(
        Query<ResultType> query, Map<String, Object> context, Segment segment
    );

    @SuppressWarnings("unchecked")
    public Sequence<ResultType> runStreaming(Query<ResultType> query, Map<String, Object> responseContext)
    {
      QueryDataSource dataSource = (QueryDataSource) query.getDataSource();

      Query<I> subQuery = dataSource.getQuery();
      Schema schema = Queries.relaySchema(subQuery, segmentWalker).asSchema(false);
      dataSource.setSchema(schema);   // will be used to resolve schema of outer query

      query = QueryUtils.resolveQuery(query, segmentWalker);

      Sequence<Row> sequence = Queries.convertToRow(subQuery, subQueryRunner.run(subQuery, responseContext));
      Cursor.WithResource cursor = ColumnSelectorFactories.toCursor(sequence, schema, query);
      if (cursor == null) {
        return Sequences.empty();
      }
      return Sequences.withBaggage(converter(query, cursor).apply(cursor), cursor);
    }

    protected Function<Cursor, Sequence<ResultType>> converter(Query<ResultType> outerQuery, Cursor cursor)
    {
      throw new UnsupportedOperationException("streaming sub-query handler");
    }

    protected final void close(Segment segment)
    {
      try {
        segment.close();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  // streaming only version
  public abstract class StreamingSubQueryRunner<I> extends SubQueryRunner<I>
  {
    protected StreamingSubQueryRunner(
        QueryRunner<I> subQueryRunner,
        QuerySegmentWalker segmentWalker,
        ExecutorService executor
    )
    {
      super(subQueryRunner, segmentWalker, executor, -1);
    }

    @Override
    public Sequence<ResultType> run(Query<ResultType> query, Map<String, Object> responseContext)
    {
      return runStreaming(query, responseContext);
    }

    @Override
    protected Function<Interval, Sequence<ResultType>> function(
        Query<ResultType> query, Map<String, Object> context, Segment segment
    )
    {
      throw new UnsupportedOperationException("sub-query handler");
    }

    @Override
    protected abstract Function<Cursor, Sequence<ResultType>> converter(
        Query<ResultType> outerQuery,
        Cursor cursor
    );
  }
}
