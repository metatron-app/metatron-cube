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

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.common.guava.Comparators;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.guava.MergeIterable;
import io.druid.java.util.common.logger.Logger;
import io.druid.utils.StopWatch;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * A QueryRunner that combines a list of other QueryRunners and executes them in parallel on an executor.
 * <p/>
 * When using this, it is important to make sure that the list of QueryRunners provided is fully flattened.
 * If, for example, you were to pass a list of a Chained QueryRunner (A) and a non-chained QueryRunner (B).  Imagine
 * A has 2 QueryRunner chained together (Aa and Ab), the fact that the Queryables are run in parallel on an
 * executor would mean that the Queryables are actually processed in the order
 * <p/>
 * <pre>A -&gt; B -&gt; Aa -&gt; Ab</pre>
 * <p/>
 * That is, the two sub queryables for A would run *after* B is run, effectively meaning that the results for B
 * must be fully cached in memory before the results for Aa and Ab are computed.
 */
public class ChainedExecutionQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(ChainedExecutionQueryRunner.class);

  private final Iterable<QueryRunner<T>> queryables;
  private final ListeningExecutorService exec;
  private final QueryWatcher queryWatcher;

  public ChainedExecutionQueryRunner(
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<T>> queryables
  )
  {
    // listeningDecorator will leave PrioritizedExecutorService unchanged,
    // since it already implements ListeningExecutorService
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryables = Iterables.unmodifiableIterable(queryables);
    this.queryWatcher = queryWatcher;
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final List<String> columns = query.estimatedInitialColumns();
    final int priority = BaseQuery.getContextPriority(query, 0);

    return Sequences.simple(columns, () -> {
      // Make it a List<> to materialize all the values (so that it will submit everything to the executor)
      ListenableFuture<List<Iterable<T>>> futures = Futures.allAsList(Iterables.transform(
          queryables,
          runner -> exec.submit(
              new AbstractPrioritizedCallable<Iterable<T>>(priority)
              {
                @Override
                public Iterable<T> call() throws Exception
                {
                  try {
                    return Sequences.toList(runner.run(query, responseContext));
                  }
                  catch (Exception e) {
                    log.error(e, "Exception with one of the sequences!");
                    throw QueryException.wrapIfNeeded(e);
                  }
                }
              }
          )
      ));

      StopWatch watch = queryWatcher.register(query, futures);
      try {
        return mergeResults(query, watch.wainOn(futures)).iterator();
      }
      catch (CancellationException e) {
        log.info("Query canceled, id [%s]", query.getId());
        Execs.cancelQuietly(futures);
        if (query.getContextBoolean("IN_TEST", false)) {
          throw QueryException.wrapIfNeeded(e);
        }
        return Collections.emptyIterator();
      }
      catch (InterruptedException e) {
        log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
        futures.cancel(true);
        throw QueryException.wrapIfNeeded(e);
      }
      catch (TimeoutException e) {
        log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
        futures.cancel(true);
        throw QueryException.wrapIfNeeded(e);
      }
      catch (ExecutionException e) {
        throw QueryException.wrapIfNeeded(e.getCause());
      }
    });
  }

  private Iterable<T> mergeResults(Query<T> query, List<Iterable<T>> results)
  {
    final Comparator<T> ordering = getMergeOrdering(query);
    return ordering == null ? Iterables.concat(results) : new MergeIterable<>(Comparators.NULL_FIRST(ordering), results);
  }

  protected Comparator<T> getMergeOrdering(Query<T> query)
  {
    return query.getMergeOrdering(null);
  }
}
