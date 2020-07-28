/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.common.guava.FutureSequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

public class QueryRunners
{
  private static final Logger LOG = new Logger(QueryWatcher.class);

  public static <T> QueryRunner<T> concat(final Iterable<QueryRunner<T>> runners)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
      {
        return Sequences.concat(Iterables.transform(runners, new Function<QueryRunner<T>, Sequence<T>>()
        {
          @Override
          public Sequence<T> apply(QueryRunner<T> runner)
          {
            return runner.run(query, responseContext);
          }
        }));
      }
    };
  }

  public static <T> QueryRunner<T> concat(final QueryRunner<T> runner, final Iterable<Query<T>> queries)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> baseQuery, final Map<String, Object> responseContext)
      {
        return Sequences.concat(
            Iterables.transform(
                queries, new Function<Query<T>, Sequence<T>>()
                {
                  @Override
                  public Sequence<T> apply(final Query<T> splitQuery)
                  {
                    return runner.run(splitQuery, responseContext);
                  }
                }
            )
        );
      }
    };
  }

  public static <T> QueryRunner<T> runWith(final Query<T> query, final QueryRunner<T> runner)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> dummy, Map<String, Object> responseContext)
      {
        return runner.run(query, responseContext);
      }
    };
  }

  public static <T> QueryRunner<T> empty()
  {
    return NoopQueryRunner.instance();
  }

  public static <T> QueryRunner<T> withResource(final QueryRunner<T> runner, final Closeable closeable)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return Sequences.withBaggage(runner.run(query, responseContext), closeable);
      }
    };
  }

  public static <T> QueryRunner<T> wrap(final Sequence<T> sequence)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return sequence;
      }
    };
  }

  public static <T> Sequence<T> run(Query<T> query, QuerySegmentWalker segmentWalker)
  {
    return query.run(segmentWalker, Maps.<String, Object>newHashMap());
  }

  @SuppressWarnings("unchecked")
  public static <T> Sequence<Object[]> run(
      Query.ArrayOutputSupport query,
      QuerySegmentWalker segmentWalker,
      Map<String, Object> responseContext
  )
  {
    return query.array(query.run(segmentWalker, Maps.<String, Object>newHashMap()));
  }

  public static <T> List<T> list(Query<T> query, QuerySegmentWalker segmentWalker)
  {
    return Sequences.toList(run(query, segmentWalker));
  }

  public static <T> T only(Query<T> query, QuerySegmentWalker segmentWalker)
  {
    return Sequences.only(run(query, segmentWalker));
  }

  public static <T> Sequence<T> run(Query<T> query, QueryRunner<T> runner)
  {
    return runner.run(query, Maps.<String, Object>newHashMap());
  }

  public static <T> QueryRunner<T> executeParallel(
      final Query<T> query,
      final ExecutorService executor,
      final List<QueryRunner<T>> runners,
      final QueryWatcher watcher
  )
  {
    if (runners.isEmpty()) {
      return QueryRunners.empty();
    }
    if (runners.size() == 1) {
      return new QueryRunner<T>()
      {
        @Override
        public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
        {
          return runners.get(0).run(query, responseContext);
        }
      };
    }
    // used for limiting resource usage from heavy aggregators like CountMinSketch
    final int parallelism = query.getContextInt(Query.MAX_QUERY_PARALLELISM, 4);
    if (parallelism < 1) {
      // no limit.. todo: deprecate this
      return new ChainedExecutionQueryRunner<T>(executor, watcher, runners);
    }
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        final int priority = BaseQuery.getContextPriority(query, 0);
        final Execs.Semaphore semaphore = new Execs.Semaphore(Math.min(parallelism, runners.size()));
        final Iterable<Callable<Sequence<T>>> works = QueryRunnerHelper.asCallable(
            runners, semaphore, query, responseContext
        );
        final List<ListenableFuture<Sequence<T>>> futures = Execs.execute(executor, works, semaphore, priority);
        final ListenableFuture<List<Sequence<T>>> future = Futures.allAsList(futures);
        final Closeable resource = () -> {
          semaphore.destroy();
          Execs.cancelQuietly(future);
        };
        if (query.getMergeOrdering() == null) {
          final ListenableFuture<Sequence<T>> first = futures.get(0);
          final List<ListenableFuture<Sequence<T>>> others = futures.subList(1, futures.size());
          final Sequence<T> sequence = waitForCompletion(query, first, watcher, resource);
          return sequence == null ? Sequences.empty() :
                 Sequences.concat(sequence, Sequences.concat(Iterables.transform(others, FutureSequence.toSequence())));
        }
        final List<Sequence<T>> sequences = waitForCompletion(query, future, watcher, resource);
        return sequences == null ? Sequences.empty() : QueryUtils.mergeSort(query, sequences);
      }
    };
  }

  public static <T> T waitForCompletion(
      final Query<?> query,
      final ListenableFuture<T> future,
      final QueryWatcher queryWatcher,
      final Closeable closeOnFailure
  )
  {
    queryWatcher.registerQuery(query, future);
    try {
      return wainOn(query, future, queryWatcher);
    }
    catch (CancellationException e) {
      LOG.info("Query [%s] is canceled", query.getId());
      IOUtils.closeQuietly(closeOnFailure);
      return null;
    }
    catch (InterruptedException | TimeoutException e) {
      String reason = e instanceof InterruptedException ? "interrupted" : "timeout";
      LOG.info("Cancelling query [%s] by reason [%s]", query.getId(), reason);
      IOUtils.closeQuietly(closeOnFailure);
      throw new QueryInterruptedException(e);
    }
    catch (QueryInterruptedException e) {
      IOUtils.closeQuietly(closeOnFailure);
      throw e;
    }
    catch (ExecutionException e) {
      IOUtils.closeQuietly(closeOnFailure);
      throw Throwables.propagate(e.getCause());
    }
  }

  public static <T> T wainOn(Query<?> query, ListenableFuture<T> future, QueryWatcher queryWatcher)
      throws InterruptedException, ExecutionException, TimeoutException
  {
    return Execs.waitOn(future, queryWatcher.remainingTime(query.getId()));
  }

  public static <I, T> QueryRunner<T> getIteratingRunner(
      final Query.IteratingQuery<I, T> iterating,
      final QuerySegmentWalker walker
  )
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, final Map<String, Object> responseContext)
      {
        return Sequences.concat(
            new Iterable<Sequence<T>>()
            {
              @Override
              public Iterator<Sequence<T>> iterator()
              {
                return new Iterator<Sequence<T>>()
                {
                  private Query<I> query = iterating.next(null, null).rhs;

                  @Override
                  public boolean hasNext()
                  {
                    return query != null;
                  }

                  @Override
                  public Sequence<T> next()
                  {
                    Sequence<I> sequence = query.run(walker, responseContext);
                    Pair<Sequence<T>, Query<I>> next = iterating.next(sequence, query);
                    query = next.rhs;
                    return next.lhs;
                  }

                  @Override
                  public void remove()
                  {
                    throw new UnsupportedOperationException("remove");
                  }
                };
              }
            }
        );
      }
    };
  }

  public static <T> QueryRunner<T> getSubQueryResolver(
      final QueryRunner<T> baseRunner,
      final QueryToolChest<T, Query<T>> toolChest,
      final QuerySegmentWalker segmentWalker,
      final QueryConfig config
  )
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        QueryDataSource dataSource = (QueryDataSource) query.getDataSource();
        Query subQuery = toolChest.prepareSubQuery(query, dataSource.getQuery());
        if (dataSource.getSchema() == null) {
          query = query.withDataSource(QueryDataSource.of(subQuery, Queries.relaySchema(subQuery, segmentWalker)));
        }
        query = QueryUtils.resolve(query, segmentWalker);
        query = QueryUtils.rewrite(query, segmentWalker, config);
        return baseRunner.run(query, responseContext);
      }
    };
  }
}
