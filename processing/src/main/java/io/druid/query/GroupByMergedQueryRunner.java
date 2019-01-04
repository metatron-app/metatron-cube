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
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.concurrent.Execs;
import io.druid.concurrent.PrioritizedRunnable;
import io.druid.data.input.Row;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.groupby.MergeIndex;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;


public class GroupByMergedQueryRunner implements QueryRunner<Row>
{
  private static final Logger log = new Logger(GroupByMergedQueryRunner.class);

  private final List<QueryRunner<Row>> queryables;
  private final ExecutorService exec;
  private final GroupByQueryConfig config;
  private final QueryWatcher queryWatcher;

  public GroupByMergedQueryRunner(
      ExecutorService exec,
      GroupByQueryConfig config,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<Row>> queryables
  )
  {
    this.exec = exec;
    this.queryWatcher = queryWatcher;
    this.queryables = Lists.newArrayList(Iterables.filter(queryables, Predicates.notNull()));
    this.config = config;
  }

  @Override
  public Sequence<Row> run(final Query<Row> queryParam, final Map<String, Object> responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryParam;

    final int maxRowCount = config.getMaxIntermediateRows();

    final ExecutorService executor;
    int parallelism = query.getContextIntWithMax(Query.GBY_MERGE_PARALLELISM, config.getMaxMergeParallelism());
    if (parallelism > 1) {
      executor = exec;
      parallelism = Math.min(Iterables.size(queryables), parallelism);
    } else {
      executor = MoreExecutors.sameThreadExecutor();
      parallelism = 1;
    }

    final MergeIndex mergeIndex = GroupByQueryHelper.createMergeIndex(query, maxRowCount, parallelism);

    final Pair<Queue, Accumulator<Queue, Row>> bySegmentAccumulatorPair = GroupByQueryHelper.createBySegmentAccumulatorPair();
    final boolean bySegment = BaseQuery.isBySegment(query);
    final int priority = BaseQuery.getContextPriority(query, 0);

    final Execs.Semaphore semaphore = new Execs.Semaphore(Math.min(queryables.size(), parallelism));

    final ListenableFuture<List<Sequence<Row>>> future = Futures.allAsList(
        Execs.execute(
            executor,
            Lists.transform(
                queryables, new Function<QueryRunner<Row>, Callable<Sequence<Row>>>()
                {
                  @Override
                  public Callable<Sequence<Row>> apply(final QueryRunner<Row> runner)
                  {
                    return new Callable<Sequence<Row>>()
                    {
                      @Override
                      public Sequence<Row> call() throws Exception
                      {
                        try {
                          Sequence<Row> sequence = Sequences.withBaggage(
                              runner.run(queryParam, responseContext),
                              semaphore
                          );
                          long start = System.currentTimeMillis();
                          if (bySegment) {
                            sequence.accumulate(bySegmentAccumulatorPair.lhs, bySegmentAccumulatorPair.rhs);
                          } else {
                            sequence.accumulate(mergeIndex, GroupByQueryHelper.<Row>newMergeAccumulator(semaphore));
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
            ), semaphore, priority
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
            mergeIndex.close();
            Execs.cancelQuietly(future);
          }
        }
    );

    if (bySegment) {
      return Sequences.simple(bySegmentAccumulatorPair.lhs);
    }

    boolean compact = !BaseQuery.isLocalFinalizingQuery(query);
    return Sequences.withBaggage(mergeIndex.toMergeStream(compact), new AsyncCloser(mergeIndex, executor));
  }

  private void waitForFutureCompletion(
      GroupByQuery query,
      ListenableFuture<?> future,
      Closeable closeOnFailure
  )
  {
    queryWatcher.registerQuery(query, future);
    try {
      long timeout = queryWatcher.remainingTime(query.getId());
      if (timeout <= 0) {
        future.get();
      } else {
        future.get(timeout, TimeUnit.MILLISECONDS);
      }
    }
    catch (CancellationException e) {
      log.info("Query canceled, id [%s]", query.getId());
      IOUtils.closeQuietly(closeOnFailure);
      // by request.. don'Row propagate
    } catch (InterruptedException | TimeoutException e) {
      String message = e instanceof InterruptedException ? "interrupted" : "timed-out";
      log.warn(e, "Query %s, cancelling pending results, query id [%s]", message, query.getId());
      IOUtils.closeQuietly(closeOnFailure);
      throw new QueryInterruptedException(e);
    } catch (QueryInterruptedException e) {
      IOUtils.closeQuietly(closeOnFailure);
      throw e;
    } catch (ExecutionException e) {
      IOUtils.closeQuietly(closeOnFailure);
      throw Throwables.propagate(e.getCause());
    }
  }

  private static class AsyncCloser implements Closeable
  {
    private final AtomicBoolean closed = new AtomicBoolean();
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
      if (!closed.compareAndSet(false, true)) {
        return;
      }
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
