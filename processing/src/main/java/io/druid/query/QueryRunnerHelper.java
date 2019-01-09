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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.cache.Cache;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.concurrent.PrioritizedCallable;
import io.druid.granularity.Granularity;
import io.druid.query.filter.DimFilter;
import io.druid.segment.Cursor;
import io.druid.segment.StorageAdapter;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 */
public class QueryRunnerHelper
{
  private static final Logger log = new Logger(QueryRunnerHelper.class);

  public static <T> Sequence<T> makeCursorBasedQuery(
      final StorageAdapter adapter,
      final Query<?> query,
      final Cache cache,
      final Function<Cursor, T> mapFn
  )
  {
    return makeCursorBasedQuery(
        adapter,
        Iterables.getOnlyElement(query.getIntervals()),
        RowResolver.of(adapter, BaseQuery.getVirtualColumns(query)),
        BaseQuery.getDimFilter(query),
        cache,
        query.isDescending(),
        query.getGranularity(),
        mapFn
    );
  }

  public static <T> Sequence<T> makeCursorBasedQuery(
      final StorageAdapter adapter,
      Interval interval,
      RowResolver resolver,
      DimFilter filter,
      Cache cache,
      boolean descending,
      Granularity granularity,
      final Function<Cursor, T> mapFn
  )
  {
    return Sequences.filter(
        Sequences.map(
            adapter.makeCursors(filter, interval, resolver, granularity, cache, descending),
            new Function<Cursor, T>()
            {
              @Override
              public T apply(Cursor input)
              {
                log.debug("Running over cursor[%s/%s]", input.getTime(), adapter.getInterval());
                return mapFn.apply(input);
              }
            }
        ),
        Predicates.<T>notNull()
    );
  }

  public static <T>  QueryRunner<T> makeClosingQueryRunner(final QueryRunner<T> runner, final Closeable closeable){
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return Sequences.withBaggage(runner.run(query, responseContext), closeable);
      }
    };
  }

  public static <T> QueryRunner<T> toManagementRunner(
      Query<T> query,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService exec,
      ObjectMapper mapper
  )
  {
    QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    exec = exec == null ? MoreExecutors.sameThreadExecutor() : exec;
    return FinalizeResultsQueryRunner.finalize(
        toolChest.mergeResults(
            factory.mergeRunners(exec, Arrays.asList(factory.createRunner(null, null)), null)
        ),
        toolChest,
        mapper
    );
  }

  public static <T> Iterable<Callable<Sequence<T>>> asCallable(
      final Iterable<QueryRunner<T>> runners,
      final Query<T> query,
      final Map<String, Object> responseContext
  )
  {
    return Iterables.transform(
        runners,
        new Function<QueryRunner<T>, Callable<Sequence<T>>>()
        {
          @Override
          public Callable<Sequence<T>> apply(final QueryRunner<T> runner)
          {
            return new PrioritizedCallable.Background<Sequence<T>>()
            {
              @Override
              public Sequence<T> call()
              {
                return runner.run(query, responseContext);
              }
            };
          }
        }
    );
  }

  public static <T> Iterable<Callable<Sequence<T>>> asCallable(
      final Iterable<QueryRunner<T>> runners,
      final Execs.Semaphore semaphore,
      final Query<T> query,
      final Map<String, Object> responseContext
  )
  {
    return Iterables.transform(
        asCallable(runners, query, responseContext),
        new Function<Callable<Sequence<T>>, Callable<Sequence<T>>>()
        {
          @Override
          public Callable<Sequence<T>> apply(final Callable<Sequence<T>> callable)
          {
            return new PrioritizedCallable.Background<Sequence<T>>()
            {
              @Override
              public Sequence<T> call() throws Exception
              {
                return Sequences.simple(Sequences.toList(Sequences.withBaggage(callable.call(), semaphore)));
              }
            };
          }
        }
    );
  }
}
