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

package io.druid.query.metadata;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.granularity.Granularity;
import io.druid.granularity.GranularityType;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.BaseQuery;
import io.druid.query.ConcatQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery.AnalysisType;
import io.druid.segment.Metadata;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SegmentMetadataQueryRunnerFactory extends QueryRunnerFactory.Abstract<SegmentAnalysis, SegmentMetadataQuery>
{
  private static final Logger log = new Logger(SegmentMetadataQueryRunnerFactory.class);

  @Inject
  public SegmentMetadataQueryRunnerFactory(
      SegmentMetadataQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public QueryRunner<SegmentAnalysis> _createRunner(final Segment segment, Future<Object> optimizer)
  {
    return new QueryRunner<SegmentAnalysis>()
    {
      @Override
      public Sequence<SegmentAnalysis> run(Query<SegmentAnalysis> inQ, Map<String, Object> responseContext)
      {
        SegmentMetadataQuery query = (SegmentMetadataQuery) inQ;

        final Map<String, ColumnAnalysis> analyzedColumns = SegmentAnalyzer.analyze(segment, query);

        final StorageAdapter adapter = segment.asStorageAdapter(false);
        final long numRows = adapter.getNumRows();

        long totalSerializedSize = -1;
        if (query.getAnalysisTypes().contains(AnalysisType.SERIALIZED_SIZE)) {
          totalSerializedSize = 0;
          for (String columnName : Iterables.concat(
              adapter.getAvailableDimensions(),
              adapter.getAvailableMetrics(),
              Arrays.asList(Column.TIME_COLUMN_NAME)
          )) {
            totalSerializedSize += adapter.getSerializedSize(columnName);
          }
        }

        List<Interval> retIntervals = query.analyzingInterval() ? Arrays.asList(segment.getInterval()) : null;

        Metadata metadata = adapter.getMetadata();

        Map<String, AggregatorFactory> aggregators = null;
        if (query.hasAggregators() && metadata != null && metadata.getAggregators() != null) {
          aggregators = AggregatorFactory.asMap(metadata.getAggregators());
        }

        Granularity queryGranularity = null;
        if (metadata != null && query.hasQueryGranularity()) {
          queryGranularity = metadata.getQueryGranularity();
        }
        Granularity segmentGranularity = null;
        if (metadata != null && query.hasQueryGranularity()) {
          segmentGranularity = metadata.getSegmentGranularity();
          if (segmentGranularity == null) {
            Interval interval = segment.getInterval();
            GranularityType granularityType = GranularityType.fromInterval(interval);
            if (granularityType != null) {
              segmentGranularity = granularityType.getDefaultGranularity();
            }
          }
        }
        long ingestedNumRows = -1;
        if (metadata != null && query.hasIngestedNumRows()) {
          ingestedNumRows = metadata.getIngestedNumRows();
        }

        long lastAccessTime = -1;
        if (query.hasLastAccessTime()) {
          lastAccessTime = segment.getLastAccessTime();
        }

        Boolean rollup = null;
        if (query.hasRollup()) {
          rollup = metadata != null ? metadata.isRollup() : null;
          if (rollup == null) {
            // in this case, this segment is built before no-rollup function is coded,
            // thus it is built with rollup
            rollup = Boolean.TRUE;
          }
        }

        return Sequences.simple(
            Arrays.asList(
                new SegmentAnalysis(
                    segment.getIdentifier(),
                    retIntervals,
                    analyzedColumns,
                    totalSerializedSize,
                    numRows,
                    ingestedNumRows,
                    lastAccessTime,
                    aggregators,
                    queryGranularity,
                    segmentGranularity,
                    rollup
                )
            )
        );
      }
    };
  }

  @Override
  public QueryRunner<SegmentAnalysis> mergeRunners(
      Query<SegmentAnalysis> query,
      ExecutorService exec,
      Iterable<QueryRunner<SegmentAnalysis>> queryRunners,
      Future<Object> optimizer
  )
  {
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(exec);
    return new ConcatQueryRunner<SegmentAnalysis>(
        Sequences.map(
            Sequences.simple(queryRunners),
            new Function<QueryRunner<SegmentAnalysis>, QueryRunner<SegmentAnalysis>>()
            {
              @Override
              public QueryRunner<SegmentAnalysis> apply(final QueryRunner<SegmentAnalysis> input)
              {
                return new QueryRunner<SegmentAnalysis>()
                {
                  @Override
                  public Sequence<SegmentAnalysis> run(
                      final Query<SegmentAnalysis> query,
                      final Map<String, Object> responseContext
                  )
                  {
                    final int priority = BaseQuery.getContextPriority(query, 0);
                    ListenableFuture<Sequence<SegmentAnalysis>> future = queryExecutor.submit(
                        new AbstractPrioritizedCallable<Sequence<SegmentAnalysis>>(priority)
                        {
                          @Override
                          public Sequence<SegmentAnalysis> call() throws Exception
                          {
                            return Sequences.simple(Sequences.toList(input.run(query, responseContext)));
                          }
                        }
                    );
                    try {
                      queryWatcher.registerQuery(query, future);
                      final long timeout = queryWatcher.remainingTime(query.getId());
                      return timeout <= 0 ? future.get() : future.get(timeout, TimeUnit.MILLISECONDS);
                    }
                    catch (CancellationException e) {
                      log.info("Query canceled, id [%s]", query.getId());
                      Execs.cancelQuietly(future);
                      return Sequences.empty();
                    }
                    catch (InterruptedException e) {
                      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
                      future.cancel(true);
                      throw new QueryInterruptedException(e);
                    }
                    catch (TimeoutException e) {
                      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
                      future.cancel(true);
                      throw new QueryInterruptedException(e);
                    }
                    catch (ExecutionException e) {
                      throw Throwables.propagate(e.getCause());
                    }
                  }
                };
              }
            }
        )
    );
  }
}
