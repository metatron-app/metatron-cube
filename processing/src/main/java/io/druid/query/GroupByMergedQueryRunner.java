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

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.collections.StupidPool;
import io.druid.common.guava.GuavaUtils;
import io.druid.concurrent.Execs;
import io.druid.concurrent.PrioritizedRunnable;
import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.groupby.MergeIndex;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class GroupByMergedQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(GroupByMergedQueryRunner.class);

  private final List<QueryRunner<T>> queryables;
  private final ExecutorService exec;
  private final Supplier<GroupByQueryConfig> configSupplier;
  private final QueryWatcher queryWatcher;
  private final StupidPool<ByteBuffer> bufferPool;
  private final Future<Object> optimizer;

  public GroupByMergedQueryRunner(
      ExecutorService exec,
      Supplier<GroupByQueryConfig> configSupplier,
      QueryWatcher queryWatcher,
      StupidPool<ByteBuffer> bufferPool,
      Iterable<QueryRunner<T>> queryables,
      Future<Object> optimizer
  )
  {
    this.exec = exec;
    this.queryWatcher = queryWatcher;
    this.queryables = Lists.newArrayList(Iterables.filter(queryables, Predicates.notNull()));
    this.configSupplier = configSupplier;
    this.bufferPool = bufferPool;
    this.optimizer = optimizer == null ? Futures.immediateFuture(null) : optimizer;
  }

  @Override
  public Sequence<T> run(final Query<T> queryParam, final Map<String, Object> responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryParam;

    final GroupByQueryConfig config = configSupplier.get();
    final int maxRowCount = config.getMaxResults();

    final ExecutorService executor;
    int parallelism = query.getContextIntWithMax(Query.GBY_MERGE_PARALLELISM, config.getMaxMergeParallelism());
    if (parallelism > 1) {
      executor = exec;
      parallelism = Math.min(Iterables.size(queryables), parallelism);
    } else {
      executor = MoreExecutors.sameThreadExecutor();
      parallelism = 1;
    }

    boolean compact = query.getContextBoolean(Query.GBY_COMPACT_TRANSFER, config.isCompactTransfer());
    if (query.getContextBoolean(Query.FINAL_WORK, true) || query.getContextBoolean(Query.FINALIZE, true)) {
      compact = false;  // direct call to historical
    }
    @SuppressWarnings("unchecked")
    final List<ValueType> groupByTypes = (List<ValueType>) Futures.getUnchecked(optimizer);
    final MergeIndex incrementalIndex = GroupByQueryHelper.createMergeIndex(
        query, bufferPool, maxRowCount, parallelism, compact, groupByTypes
    );
    if (groupByTypes != null) {
      responseContext.put(Result.GROUPBY_TYPES_KEY, groupByTypes);
    }

    final Pair<Queue, Accumulator<Queue, T>> bySegmentAccumulatorPair = GroupByQueryHelper.createBySegmentAccumulatorPair();
    final boolean bySegment = BaseQuery.getContextBySegment(query, false);
    final int priority = BaseQuery.getContextPriority(query, 0);

    final Execs.Semaphore semaphore = new Execs.Semaphore(Math.min(queryables.size(), parallelism));

    ListenableFuture<List<Sequence<T>>> future = Futures.allAsList(
        Execs.execute(
            executor,
            Lists.transform(
                queryables, new Function<QueryRunner<T>, Callable<Sequence<T>>>()
                {
                  @Override
                  public Callable<Sequence<T>> apply(final QueryRunner<T> runner)
                  {
                    return new AbstractPrioritizedCallable<Sequence<T>>(priority)
                    {
                      @Override
                      public Sequence<T> call() throws Exception
                      {
                        try {
                          Sequence<T> sequence = new ResourceClosingSequence<T>(
                              runner.run(queryParam, responseContext),
                              semaphore
                          );
                          long start = System.currentTimeMillis();
                          if (bySegment) {
                            sequence.accumulate(bySegmentAccumulatorPair.lhs, bySegmentAccumulatorPair.rhs);
                          } else {
                            sequence.accumulate(incrementalIndex, GroupByQueryHelper.<T>newMergeAccumulator(semaphore));
                          }
                          log.debug("accumulated in %,d msec", (System.currentTimeMillis() - start));
                          return null;
                        }
                        catch (QueryInterruptedException e) {
                          throw Throwables.propagate(e);
                        }
                        catch (Exception e) {
                          log.error(e, "Exception with one of the sequences!");
                          throw Throwables.propagate(e);
                        }
                      }
                    };
                  }
                }
            ), semaphore, parallelism, priority
        )
    );
    waitForFutureCompletion(
        query,
        future,
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            semaphore.destroy();
            incrementalIndex.close();
          }
        }
    );

    if (bySegment) {
      return Sequences.simple(bySegmentAccumulatorPair.lhs);
    }

    return new ResourceClosingSequence<T>(
        Sequences.simple(
            Iterables.<Row, T>transform(
                incrementalIndex.toMergeStream(),
                GuavaUtils.<Row, T>caster()
            )
        ), new AsyncCloser(incrementalIndex, executor)
    );
  }

  private void waitForFutureCompletion(
      GroupByQuery query,
      ListenableFuture<?> future,
      Closeable closeOnFailure
  )
  {
    try {
      queryWatcher.registerQuery(query, future);
      final Number timeout = query.getContextValue(QueryContextKeys.TIMEOUT, (Number) null);
      if (timeout == null) {
        future.get();
      } else {
        future.get(timeout.longValue(), TimeUnit.MILLISECONDS);
      }
    }
    catch (InterruptedException | TimeoutException | CancellationException e) {
      if (e instanceof CancellationException) {
        log.info("Query canceled, id [%s]", query.getId());
      } else {
        String message = e instanceof InterruptedException ? "interrupted" : "timed-out";
        log.warn(e, "Query %s, cancelling pending results, query id [%s]", message, query.getId());
      }
      future.cancel(true);
      IOUtils.closeQuietly(closeOnFailure);
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
      IOUtils.closeQuietly(closeOnFailure);
      throw Throwables.propagate(e.getCause());
    }
  }

  private static class AsyncCloser implements Closeable
  {
    private final Closeable delegate;
    private final ExecutorService exec;

    private AsyncCloser(Closeable delegate, ExecutorService exec)
    {
      this.delegate = delegate;
      this.exec = exec;
    }

    @Override
    public void close() throws IOException
    {
      exec.submit(
          new PrioritizedRunnable.Zero()
          {
            @Override
            public void run()
            {
              try {
                delegate.close();
              }
              catch (IOException e) {
                log.debug(e, "Failed to close merge index..");
              }
            }
          }
      );
    }
  }
}
