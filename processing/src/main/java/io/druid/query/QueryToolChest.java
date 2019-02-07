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
import com.google.common.base.Throwables;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.select.Schema;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.Cursor;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.LogicalSegment;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

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
  protected static final byte SCHEMA_QUERY = 0x18;

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
   * @return A MetricEvent.Builder that can be used to make metrics for the provided query
   */
  public Function<QueryType, ServiceMetricEvent.Builder> makeMetricBuilder()
  {
    return new Function<QueryType, ServiceMetricEvent.Builder>()
    {
      @Override
      public ServiceMetricEvent.Builder apply(QueryType query)
      {
        return DruidMetrics.makePartialQueryTimeMetric(query);
      }
    };
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
   * Simple utility method for deserializing non by-segment result (see DDC)
   *
   * @param query
   * @param sequence
   *
   * @return
   */
  public Sequence<ResultType> deserializeSequence(QueryType query, Sequence<ResultType> sequence)
  {
    return Sequences.map(sequence, makePreComputeManipulatorFn(query, MetricManipulatorFns.deserializing()));
  }

  public Sequence<ResultType> serializeSequence(
      QueryType query,
      Sequence<ResultType> sequence,
      QuerySegmentWalker segmentWalker
  )
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

  private static final ToIntFunction COUNTER = new ToIntFunction()
  {
    @Override
    public int applyAsInt(Object value) { return 1;}
  };

  @SuppressWarnings("unchecked")
  public static ToIntFunction numRows(Query query, QueryToolChest toolChest)
  {
    return toolChest == null ? QueryToolChest.COUNTER : toolChest.numRows(query);
  }

  public ToIntFunction numRows(QueryType query)
  {
    return COUNTER;
  }

  public final <X> CacheStrategy<ResultType, X, QueryType> getCacheStrategyIfExists(QueryType query)
  {
    if (this instanceof CacheSupport) {
      return ((CacheSupport<ResultType, X, QueryType>) this).getCacheStrategy(query);
    }
    return null;
  }

  public static abstract class CacheSupport<ResultType, X, QueryType extends Query<ResultType>>
      extends QueryToolChest<ResultType, QueryType>
  {
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
    public abstract CacheStrategy<ResultType, X, QueryType> getCacheStrategy(QueryType query);
  }

  protected abstract class IdentityCacheStrategy extends CacheStrategy.Identity<ResultType, QueryType>
  {
    @Override
    public TypeReference<ResultType> getCacheObjectClazz()
    {
      return getResultTypeReference();
    }
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
  public QueryRunner<ResultType> finalizeResults(QueryRunner<ResultType> runner)
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
  public <I> Query<I> prepareSubQuery(Query<ResultType> outerQuery, Query<I> innerQuery)
  {
    innerQuery = innerQuery.withOverriddenContext(BaseQuery.copyContextForMeta(outerQuery.getContext()));
    if (innerQuery instanceof Query.DimFilterSupport) {
      // todo pushdown predicate if possible (need input schema)
    }
    // don't finalize result of sub-query
    return innerQuery.withOverriddenContext(Query.FINALIZE, false);
  }

  /**
   * @param segmentWalker
   * @param config
   */
  public <I> QueryRunner<ResultType> handleSubQuery(QuerySegmentWalker segmentWalker, QueryConfig config)
  {
    throw new UnsupportedOperationException("handleSourceQuery");
  }

  protected abstract class SubQueryRunner<I> implements QueryRunner<ResultType>
  {
    protected final QuerySegmentWalker segmentWalker;
    protected final QueryConfig config;

    protected SubQueryRunner(QuerySegmentWalker segmentWalker, QueryConfig config)
    {
      this.segmentWalker = segmentWalker;
      this.config = config;
    }

    @Override
    public Sequence<ResultType> run(Query<ResultType> query, Map<String, Object> responseContext)
    {
      IncrementalIndex accumulated = accumulate(query, responseContext);
      if (accumulated.isEmpty()) {
        return Sequences.empty();
      }
      String sources = StringUtils.join(query.getDataSource().getNames(), '_');
      StorageAdapter adapter = new IncrementalIndexStorageAdapter.Temporary(sources, accumulated);
      Segment segment = new IncrementalIndexSegment(accumulated, adapter.getSegmentIdentifier());

      Sequence<Sequence<ResultType>> sequences = Sequences.map(
          Sequences.simple(query.getIntervals()),
          query(query, segment)
      );
      return mergeQuery(query, sequences, segment);
    }

    @SuppressWarnings("unchecked")
    protected IncrementalIndex accumulate(Query<ResultType> query, Map<String, Object> responseContext)
    {
      long start = System.currentTimeMillis();
      QueryDataSource dataSource = (QueryDataSource) query.getDataSource();

      Query subQuery = dataSource.getQuery();
      Sequence<Row> innerSequence = Queries.convertToRow(subQuery, subQuery.run(segmentWalker, responseContext));

      Schema schema = dataSource.getSchema();
      LOG.info(
          "Accumulating into intermediate index with dimensions [%s] and metrics [%s]",
          schema.dimensionAndTypesString(), schema.metricAndTypesString()
      );
      int maxResult = config.getMaxResults(query);
      IncrementalIndex index = new OnheapIncrementalIndex(schema.asRelaySchema(), false, true, true, false, maxResult);
      IncrementalIndex accumulated = innerSequence.accumulate(index, GroupByQueryHelper.<Row>newIndexAccumulator());
      LOG.info(
          "Accumulated sub-query into index in %,d msec.. total %,d rows",
          System.currentTimeMillis() - start, accumulated.size()
      );
      return accumulated;
    }

    protected abstract Function<Interval, Sequence<ResultType>> query(Query<ResultType> query, Segment segment);

    protected Sequence<ResultType> mergeQuery(
        Query<ResultType> query,
        Sequence<Sequence<ResultType>> sequences,
        Segment segment
    )
    {
      return Sequences.withBaggage(Sequences.concat(sequences), segment);
    }

    @SuppressWarnings("unchecked")
    public Sequence<ResultType> runStreaming(Query<ResultType> query, Map<String, Object> responseContext)
    {
      QueryDataSource dataSource = (QueryDataSource) query.getDataSource();

      Query subQuery = dataSource.getQuery();
      Sequence<Row> sequence = Queries.convertToRow(subQuery, subQuery.run(segmentWalker, responseContext));

      Sequence<Cursor> cursors = ColumnSelectorFactories.toCursor(sequence, dataSource.getSchema(), query);
      return streamMerge(Sequences.map(cursors, streamQuery(query)));
    }

    protected Function<Cursor, Sequence<ResultType>> streamQuery(Query<ResultType> query)
    {
      throw new UnsupportedOperationException("streaming sub-query handler");
    }

    protected Sequence<ResultType> streamMerge(Sequence<Sequence<ResultType>> sequences)
    {
      return Sequences.concat(sequences);
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
    protected StreamingSubQueryRunner(QuerySegmentWalker segmentWalker, QueryConfig config)
    {
      super(segmentWalker, config);
    }

    @Override
    public Sequence<ResultType> run(Query<ResultType> query, Map<String, Object> responseContext)
    {
      return runStreaming(query, responseContext);
    }

    @Override
    protected Function<Interval, Sequence<ResultType>> query(Query<ResultType> query, Segment segment)
    {
      throw new UnsupportedOperationException("streaming only sub-query handler");
    }

    @Override
    protected abstract Function<Cursor, Sequence<ResultType>> streamQuery(Query<ResultType> query);
  }
}
