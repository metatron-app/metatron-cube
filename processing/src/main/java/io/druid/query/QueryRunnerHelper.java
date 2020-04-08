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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.cache.Cache;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.concurrent.PrioritizedCallable;
import io.druid.segment.Cursor;
import io.druid.segment.CursorFactory;

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
      final CursorFactory factory,
      final Query<?> query,
      final Cache cache,
      final Function<Cursor, T> mapFn
  )
  {
    return Sequences.filterNull(
        Sequences.map(factory.makeCursors(query, cache), mapFn)
    );
  }

  public static <T> Sequence<T> makeCursorBasedQueryConcat(
      final CursorFactory factory,
      final Query<?> query,
      final Cache cache,
      final Function<Cursor, Sequence<T>> mapFn
  )
  {
    return Sequences.concat(Sequences.filterNull(
        Sequences.map(factory.makeCursors(query, cache), mapFn)
    ));
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
            factory.mergeRunners(query, exec, Arrays.asList(factory.createRunner(null, null)), null)
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
