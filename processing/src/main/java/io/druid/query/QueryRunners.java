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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.FutureSequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class QueryRunners
{
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
    return new NoopQueryRunner<>();
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
      final ExecutorService executor,
      final List<QueryRunner<T>> runners,
      final Ordering<T> ordering
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
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        final int priority = BaseQuery.getContextPriority(query, 0);
        final Execs.Semaphore semaphore = new Execs.Semaphore(Math.min(4, runners.size()));
        final Iterable<Sequence<T>> sequences = Iterables.transform(
            Execs.execute(
                executor,
                QueryRunnerHelper.asCallable(runners, semaphore, query, responseContext),
                semaphore,
                priority
            ),
            FutureSequence.<T>toSequence()
        );
        return ordering == null ? Sequences.concat(sequences) : Sequences.mergeSort(ordering, sequences);
      }
    };
  }
}
