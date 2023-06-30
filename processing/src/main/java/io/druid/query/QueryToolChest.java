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

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Function;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query.ArrayOutputSupport;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.Cursor;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import io.druid.timeline.LogicalSegment;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.ToIntFunction;

/**
 * The broker-side (also used by server in some cases) API for a specific Query type.  This API is still undergoing
 * evolution and is only semi-stable, so proprietary Query implementations should be ready for the potential
 * maintenance burden when upgrading versions.
 */
public abstract class QueryToolChest<T>
{
  protected static final TypeReference<Object[]> ARRAY_TYPE_REFERENCE = new TypeReference<Object[]>() {};
  protected static final TypeReference<Row> ROW_TYPE_REFERENCE = new TypeReference<Row>() {};
  protected static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

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
  protected static final byte SCHEMA_QUERY = 0x18;
  protected static final byte FILTER_META_QUERY = 0x19;

  /**
   * This method wraps a QueryRunner.  The input QueryRunner, by contract, will provide a series of
   * ResultType objects in time order (ascending or descending).  This method should return a new QueryRunner that
   * potentially merges the stream of ordered ResultType objects.
   *
   * @param runner A QueryRunner that provides a series of ResultType objects in time order (ascending or descending)
   *
   * @return a QueryRunner that potentially merges the stream of ordered ResultType objects
   */
  public abstract QueryRunner<T> mergeResults(QueryRunner<T> runner);

  /**
   * Creates a {@link QueryMetrics} object that is used to generate metrics for this specific query type.  This exists
   * to allow for query-specific dimensions and metrics.  That is, the ToolChest is expected to set some
   * meaningful dimensions for metrics given this query type.  Examples might be the topN threshold for
   * a TopN query or the number of dimensions included for a groupBy query.
   * 
   * <p>QueryToolChests for query types in core (druid-processing) and public extensions (belonging to the Druid source
   * tree) should use delegate this method to {@link GenericQueryMetricsFactory#makeMetrics(Query)} on an injected
   * instance of {@link GenericQueryMetricsFactory}, as long as they don't need to emit custom dimensions and/or
   * metrics.
   *
   * <p>If some custom dimensions and/or metrics should be emitted for a query type, a plan described in
   * "Making subinterfaces of QueryMetrics" section in {@link QueryMetrics}'s class-level Javadocs should be followed.
   *
   * <p>One way or another, this method should ensure that {@link QueryMetrics#query(Query)} is called with the given
   * query passed on the created QueryMetrics object before returning.
   *
   * @param query The query that is being processed
   *
   * @return A QueryMetrics that can be used to make metrics for the provided query
   */
  public abstract QueryMetrics makeMetrics(Query<T> query);

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
  public Function<T, T> makePreComputeManipulatorFn(Query<T> query, MetricManipulationFn fn)
  {
    // called with local query
    return GuavaUtils.identity("preCompute");
  }

  /**
   * Simple utility method for deserializing non by-segment result (see DDC)
   *
   * @param query
   * @param sequence
   *
   * @param executor
   * @return
   */
  @SuppressWarnings("unchecked")
  public Sequence<T> deserializeSequence(Query<T> query, Sequence sequence, ExecutorService executor)
  {
    return Sequences.map(sequence, makePreComputeManipulatorFn(query, MetricManipulatorFns.deserializing()));
  }

  public Sequence serializeSequence(Query<T> query, Sequence<T> sequence, QuerySegmentWalker segmentWalker)
  {
    return sequence;
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
  public Function<T, T> makePostComputeManipulatorFn(Query<T> query, MetricManipulationFn fn)
  {
    return GuavaUtils.identity("postCompute");
  }

  /**
   * Returns a TypeReference object that is just passed through to Jackson in order to deserialize
   * the results of this type of query.
   *
   * @return A TypeReference to indicate to Jackson what type of data will exist for this query
   * @param query
   */
  public JavaType getResultTypeReference(Query<T> query, TypeFactory factory)
  {
    JavaType baseType = factory.constructType(getResultTypeReference(query));
    if (query != null && BaseQuery.isBySegment(query)) {
      return factory.constructParametricType(
          Result.class, factory.constructParametricType(BySegmentResultValueClass.class, baseType)
      );
    }
    return baseType;
  }

  protected abstract TypeReference<T> getResultTypeReference(Query<T> query);

  public BySegmentResultValue<T> bySegment(
      Query<T> query,
      Sequence<T> sequence,
      String segmentId
  )
  {
    return new BySegmentResultValueClass<T>(
        Sequences.toList(sequence),
        segmentId,
        query.getIntervals().get(0)
    );
  }

  @SuppressWarnings("unchecked")
  public static QueryMetrics getQueryMetrics(Query query, QueryToolChest toolChest)
  {
    return toolChest.makeMetrics(query);
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
  public QueryRunner<T> preMergeQueryDecoration(QueryRunner<T> runner)
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
  public QueryRunner<T> postMergeQueryDecoration(QueryRunner<T> runner)
  {
    return runner;
  }

  public QueryRunner<T> finalizeResults(QueryRunner<T> runner)
  {
    return new FinalizeResultsQueryRunner<>(runner, this);
  }

  public QueryRunner<T> finalQueryDecoration(QueryRunner<T> runner)
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
  public <S extends LogicalSegment> List<S> filterSegments(Query<T> query, List<S> segments)
  {
    return segments;
  }

  public Query<T> optimizeQuery(Query<T> query, QuerySegmentWalker walker)
  {
    return query;
  }

  /**
   * converts result to tabular format to be stored file system.
   * currently, only select and group-by query supports this.
   *
   * @param query
   * @return
   */
  public Function<Sequence<T>, Sequence<Map<String, Object>>> asMap(Query<T> query, String timestampColumn)
  {
    throw new UnsupportedOperationException("asMap");
  }

  public <I> Query<I> prepareSubQuery(Query<T> outerQuery, Query<I> innerQuery)
  {
    innerQuery = innerQuery.withOverriddenContext(BaseQuery.copyContextForMeta(outerQuery.getContext()));
    if (innerQuery instanceof Query.FilterSupport) {
      // todo pushdown predicate if possible (need input schema)
    }
    // don't finalize result of sub-query
    return innerQuery.withOverriddenContext(Query.FINALIZE, false);
  }

  /**
   * @param segmentWalker
   *
   */
  public <I> QueryRunner<T> handleSubQuery(QuerySegmentWalker segmentWalker)
  {
    throw new UnsupportedOperationException("handleSourceQuery");
  }

  protected abstract class SubQueryRunner<I> implements QueryRunner<T>
  {
    protected final QuerySegmentWalker segmentWalker;

    protected SubQueryRunner(QuerySegmentWalker segmentWalker)
    {
      this.segmentWalker = segmentWalker;
    }

    @Override
    public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
    {
      IncrementalIndex accumulated = accumulate(query, responseContext);
      if (accumulated.isEmpty()) {
        return Sequences.empty();
      }
      String sources = StringUtils.join(query.getDataSource().getNames(), '_');
      String id = DataSegment.toSegmentId(sources, accumulated.getInterval(), "temporary", 0);
      Segment segment = new IncrementalIndexSegment(accumulated, DataSegment.asKey(id));

      Sequence<Sequence<T>> sequences = Sequences.map(
          Sequences.simple(query.getIntervals()),
          query(query, segment)
      );
      return mergeQuery(query, sequences, segment);
    }

    @SuppressWarnings("unchecked")
    private IncrementalIndex accumulate(Query<T> query, Map<String, Object> responseContext)
    {
      long start = System.currentTimeMillis();
      QueryDataSource dataSource = (QueryDataSource) query.getDataSource();

      Query subQuery = dataSource.getQuery();
      Sequence<Row> sequence = Queries.convertToRow(subQuery, subQuery.run(segmentWalker, responseContext));

      RowSignature schema = dataSource.getSchema();
      LOG.info(
          "Accumulating into intermediate index with dimensions [%s] and metrics [%s]",
          schema.dimensionAndTypes(), schema.metricAndTypes()
      );
      int maxResult = segmentWalker.getConfig().getMaxResults(query);
      IncrementalIndex index = new OnheapIncrementalIndex(IncrementalIndexSchema.from(schema), false, true, false, maxResult);
      IncrementalIndex accumulated = sequence.accumulate(index, GroupByQueryHelper.<Row>newIndexAccumulator());
      LOG.info(
          "Accumulated sub-query into index in %,d msec.. total %,d rows",
          System.currentTimeMillis() - start, accumulated.size()
      );
      return accumulated;
    }

    protected abstract Function<Interval, Sequence<T>> query(Query<T> query, Segment segment);

    protected Sequence<T> mergeQuery(
        Query<T> query,
        Sequence<Sequence<T>> sequences,
        Segment segment
    )
    {
      return Sequences.withBaggage(Sequences.concat(query.estimatedOutputColumns(), sequences), segment);
    }

    @SuppressWarnings("unchecked")
    public Sequence<T> runStreaming(Query<T> query, Map<String, Object> responseContext)
    {
      QueryDataSource dataSource = (QueryDataSource) query.getDataSource();
      Query subQuery = dataSource.getQuery();

      String timeColumn = Row.TIME_COLUMN_NAME;
      if (subQuery instanceof JoinQuery.JoinHolder) {
        timeColumn = ((JoinQuery.JoinHolder) subQuery).getTimeColumnName();
      }

      Sequence<Cursor> cursors;
      if (subQuery instanceof ArrayOutputSupport) {
        ArrayOutputSupport array = (ArrayOutputSupport) subQuery;
        Sequence<Object[]> sequence = QueryRunners.runArray(array, segmentWalker, responseContext);
        cursors = ColumnSelectorFactories.toArrayCursors(sequence, dataSource.getSchema(), timeColumn, query);
      } else {
        Sequence<Row> sequence = Queries.convertToRow(subQuery, subQuery.run(segmentWalker, responseContext));
        cursors = ColumnSelectorFactories.toRowCursors(sequence, dataSource.getSchema(), query);
      }
      return streamMerge(query, Sequences.map(cursors, streamQuery(query)));
    }

    protected Function<Cursor, Sequence<T>> streamQuery(Query<T> query)
    {
      throw new UnsupportedOperationException("streaming sub-query handler");
    }

    protected Sequence<T> streamMerge(Query<T> query, Sequence<Sequence<T>> sequences)
    {
      return Sequences.concat(query.estimatedOutputColumns(), sequences);
    }

    protected final void close(Segment segment)
    {
      try {
        segment.close();
      }
      catch (Exception e) {
        throw QueryException.wrapIfNeeded(e);
      }
    }
  }

  // streaming only version
  public abstract class StreamingSubQueryRunner<I> extends SubQueryRunner<I>
  {
    protected StreamingSubQueryRunner(QuerySegmentWalker segmentWalker)
    {
      super(segmentWalker);
    }

    @Override
    public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
    {
      return runStreaming(query, responseContext);
    }

    @Override
    protected final Function<Interval, Sequence<T>> query(Query<T> query, Segment segment)
    {
      throw new UnsupportedOperationException("streaming only sub-query handler");
    }

    @Override
    protected abstract Function<Cursor, Sequence<T>> streamQuery(Query<T> query);
  }

  @SuppressWarnings("unchecked")
  public final <X, Q> CacheStrategy<T, X, Q> getCacheStrategyIfExists(Query<T> query)
  {
    if (this instanceof CacheSupport) {
      return ((CacheSupport<T, X, Q>) this).getCacheStrategy((Q) query);
    }
    return null;
  }

  public static abstract class CacheSupport<T, X, Q> extends QueryToolChest<T>
  {
    /**
     * Returns a CacheStrategy to be used to load data into the cache and remove it from the cache.
     * <p>
     * This is optional.  If it returns null, caching is effectively disabled for the query.
     *
     * @param query The query whose results might be cached
     *
     * @return A CacheStrategy that can be used to populate and read from the Cache
     */
    public abstract CacheStrategy<T, X, Q> getCacheStrategy(Q query);
  }

  protected abstract class IdenticalCacheStrategy<Q> implements CacheStrategy.Identitcal<T, Q>
  {
    @Override
    public final TypeReference<T> getCacheObjectClazz()
    {
      return getResultTypeReference(null);
    }
  }

  private static final ToIntFunction ROW_COUNTER = v -> 1;

  @SuppressWarnings("unchecked")
  public static ToIntFunction numRows(Query query, QueryToolChest toolChest)
  {
    return toolChest == null ? ROW_COUNTER : toolChest.numRows(query);
  }

  public ToIntFunction numRows(Query<T> query)
  {
    return ROW_COUNTER;
  }

  public static abstract class MetricSupport<T> extends QueryToolChest<T>
  {
    private final GenericQueryMetricsFactory metricsFactory;

    protected MetricSupport(GenericQueryMetricsFactory metricsFactory)
    {
      this.metricsFactory = metricsFactory;
    }

    @Override
    public QueryMetrics makeMetrics(Query<T> query)
    {
      return metricsFactory.makeMetrics(query);
    }
  }
}
