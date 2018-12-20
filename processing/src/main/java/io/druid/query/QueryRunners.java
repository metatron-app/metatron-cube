package io.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;

import java.util.Map;

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
}
