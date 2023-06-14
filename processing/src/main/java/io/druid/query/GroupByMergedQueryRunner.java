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

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.input.Row;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.groupby.MergeIndex;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class GroupByMergedQueryRunner implements QueryRunner<Row>
{
  private static final Logger log = new Logger(GroupByMergedQueryRunner.class);

  private final List<QueryRunner<Row>> queryables;
  private final ExecutorService exec;
  private final QueryConfig config;
  private final QueryWatcher queryWatcher;

  public GroupByMergedQueryRunner(
      ExecutorService exec,
      QueryConfig config,
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
    if (queryables.isEmpty()) {
      return Sequences.empty();
    }
    final GroupByQuery query = (GroupByQuery) queryParam;

    final ExecutorService executor;
    int parallelism = config.getQueryParallelism(query);
    if (parallelism > 1) {
      executor = exec;
      parallelism = Math.min(queryables.size(), parallelism);
    } else {
      executor = Execs.newDirectExecutorService();
      parallelism = 1;
    }

    final MergeIndex mergeIndex = GroupByQueryHelper.createMergeIndex(query, config, parallelism);
    final Execs.Semaphore semaphore = new Execs.Semaphore(parallelism);
    final int priority = BaseQuery.getContextPriority(query, 0);
    final ListenableFuture<List<Sequence>> future = Futures.allAsList(
        Execs.execute(
            executor,
            Lists.transform(
                queryables, new Function<QueryRunner, Callable<Sequence>>()
                {
                  @Override
                  public Callable<Sequence> apply(final QueryRunner runner)
                  {
                    return new Callable<Sequence>()
                    {
                      @Override
                      @SuppressWarnings("unchecked")
                      public Sequence call() throws Exception
                      {
                        final Sequence sequence = Sequences.withBaggage(runner.run(query, responseContext), semaphore);
                        try {
                          long start = System.currentTimeMillis();
                          sequence.accumulate(mergeIndex, GroupByQueryHelper.newMergeAccumulator(semaphore));
                          log.debug("accumulated in %,d msec", (System.currentTimeMillis() - start));
                          return null;
                        }
                        catch (Exception e) {
                          log.error(e, "Exception with one of the sequences!");
                          throw QueryException.wrapIfNeeded(e);
                        }
                      }
                    };
                  }
                }
            ), semaphore, priority
        )
    );

    Closeable resource = () -> {
      semaphore.destroy();
      mergeIndex.close();
      Execs.cancelQuietly(future);
    };
    QueryRunners.waitForCompletion(query, future, queryWatcher, resource);

    boolean parallel = config.useParallelSort(query);
    boolean compact = !BaseQuery.isLocalFinalizingQuery(query);
    return Sequences.withBaggage(mergeIndex.toMergeStream(parallel, compact), new AsyncCloser(resource, executor));
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
      exec.submit(() -> {
        try {
          delegate.close();
        }
        catch (IOException e) {
          log.debug(e, "Failed to close merge index..");
        }
      });
    }
  }
}
