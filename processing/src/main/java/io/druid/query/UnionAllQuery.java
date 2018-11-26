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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.Pair;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.FutureSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 */
public class UnionAllQuery<T> extends BaseQuery<T> implements Query.RewritingQuery<T>
{
  private static final Logger LOG = new Logger(UnionAllQuery.class);

  // dummy datasource for authorization
  private static <T> DataSource unionDataSource(Query<T> query, List<Query<T>> queries)
  {
    if (queries == null || queries.isEmpty()) {
      return Preconditions.checkNotNull(query).getDataSource();
    }
    Set<String> names = Sets.newLinkedHashSet();
    for (Query q : queries) {
      names.addAll(q.getDataSource().getNames());
    }
    return UnionDataSource.of(names);
  }

  // dummy datasource for authorization
  private static <T> QuerySegmentSpec unionQuerySegmentSpec(Query<T> query, List<Query<T>> queries)
  {
    if (queries == null || queries.isEmpty()) {
      return Preconditions.checkNotNull(query).getQuerySegmentSpec();
    }
    List<Interval> intervals = Lists.newArrayList();
    for (Query q : queries) {
      intervals.addAll(q.getQuerySegmentSpec().getIntervals());
    }
    return new MultipleIntervalSegmentSpec(JodaUtils.condenseIntervals(intervals));
  }

  private final Query<T> query;
  private final List<Query<T>> queries;
  private final boolean sortOnUnion;
  private final int limit;
  private final int parallelism;

  @JsonCreator
  public UnionAllQuery(
      @JsonProperty("query") Query<T> query,
      @JsonProperty("queries") List<Query<T>> queries,
      @JsonProperty("sortOnUnion") boolean sortOnUnion,
      @JsonProperty("limit") int limit,
      @JsonProperty("parallelism") int parallelism,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(unionDataSource(query, queries), unionQuerySegmentSpec(query, queries), false, context);
    this.query = query;
    this.queries = queries;
    this.sortOnUnion = sortOnUnion;
    this.limit = limit;
    this.parallelism = parallelism;
  }

  public Query getRepresentative()
  {
    if (queries == null || queries.isEmpty()) {
      return Preconditions.checkNotNull(query);
    }
    Preconditions.checkArgument(query == null);
    Preconditions.checkArgument(!Iterables.contains(queries, null), "should not contain null query in union");
    Query<T> first = queries.get(0);
    for (int i = 1; i < queries.size(); i++) {
      if (!first.getType().equals(queries.get(i).getType())) {
        throw new IllegalArgumentException("sub queries in union should not be mixed");
      }
    }
    return first;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<Query<T>> getQueries()
  {
    return queries;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Query<T> getQuery()
  {
    return query;
  }

  @JsonProperty
  public boolean isSortOnUnion()
  {
    return sortOnUnion;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @JsonProperty
  public int getParallelism()
  {
    return parallelism;
  }

  @Override
  public boolean hasFilters()
  {
    if (query != null) {
      return query.hasFilters();
    }
    for (Query q : queries) {
      if (q.hasFilters()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getType()
  {
    return Query.UNION_ALL;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query<T> withOverriddenContext(final Map<String, Object> contextOverride)
  {
    return newInstance(query, queries, computeOverriddenContext(contextOverride));
  }

  @SuppressWarnings("unchecked")
  protected Query newInstance(Query<T> query, List<Query<T>> queries, Map<String, Object> context)
  {
    return new UnionAllQuery(query, queries, sortOnUnion, limit, parallelism, context);
  }

  @Override
  public Query<T> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new IllegalStateException();
  }

  @Override
  public Query<T> withDataSource(DataSource dataSource)
  {
    throw new IllegalStateException();
  }

  @SuppressWarnings("unchecked")
  public Query withQueries(List<Query> queries)
  {
    return new UnionAllQuery(null, queries, sortOnUnion, limit, parallelism, getContext());
  }

  @SuppressWarnings("unchecked")
  public Query withQuery(Query query)
  {
    return new UnionAllQuery(query, null, sortOnUnion, limit, parallelism, getContext());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    UnionAllQuery that = (UnionAllQuery) o;

    if (sortOnUnion != that.sortOnUnion) {
      return false;
    }
    if (queries != null ? !queries.equals(that.queries) : that.queries != null) {
      return false;
    }
    if (query != null ? !query.equals(that.query) : that.query != null) {
      return false;
    }
    if (limit != that.limit) {
      return false;
    }
    if (parallelism != that.parallelism) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (query != null ? query.hashCode() : 0);
    result = 31 * result + (queries != null ? queries.hashCode() : 0);
    result = 31 * result + (sortOnUnion ? 1 : 0);
    result = 31 * result + limit;
    result = 31 * result + parallelism;
    return result;
  }

  @Override
  public String toString()
  {
    return "UnionAllQuery{" +
           (query != null ? "query=" + query : "queries=" + queries) +
           ", sortOnUnion=" + sortOnUnion +
           ", limit=" + limit +
           ", parallelism=" + parallelism +
           '}';
  }


  @SuppressWarnings("unchecked")
  public QueryRunner<T> getUnionQueryRunner(
      final ObjectMapper mapper,
      final ExecutorService exec,
      final QuerySegmentWalker segmentWalker
  )
  {
    final UnionAllQueryRunner<T> baseRunner;
    if (parallelism <= 1) {
     // executes when the first element of the sequence is accessed
     baseRunner = new UnionAllQueryRunner<T>()
      {
        @Override
        public Sequence<Pair<Query<T>, Sequence<T>>> run(final Query<T> query, final Map<String, Object> responseContext)
        {
          final List<Query<T>> ready = toTargetQueries();
          return Sequences.simple(
              Lists.transform(
                  ready,
                  new Function<Query<T>, Pair<Query<T>, Sequence<T>>>()
                  {
                    @Override
                    public Pair<Query<T>, Sequence<T>> apply(final Query<T> query)
                    {
                      return Pair.<Query<T>, Sequence<T>>of(
                          query, new LazySequence<T>(
                              new Supplier<Sequence<T>>()
                              {
                                @Override
                                public Sequence<T> get()
                                {
                                  return query.run(segmentWalker, responseContext);
                                }
                              }
                          )
                      );
                    }
                  }
              )
          );
        }
      };
    } else {
      // executing now
      baseRunner = new UnionAllQueryRunner<T>()
      {
        final int priority = BaseQuery.getContextPriority(UnionAllQuery.this, 0);
        @Override
        public Sequence<Pair<Query<T>, Sequence<T>>> run(final Query<T> query, final Map<String, Object> responseContext)
        {
          final List<Query<T>> ready = toTargetQueries();
          final Execs.Semaphore semaphore = new Execs.Semaphore(Math.min(getParallelism(), ready.size()));
          LOG.info("Starting %d parallel works with %d threads", ready.size(), semaphore.availablePermits());
          final List<ListenableFuture<Sequence<T>>> futures = Execs.execute(
              exec, Lists.transform(
                  ready, new Function<Query<T>, Callable<Sequence<T>>>()
                  {
                    @Override
                    public Callable<Sequence<T>> apply(final Query<T> query)
                    {
                      return new Callable<Sequence<T>>()
                      {
                        @Override
                        public Sequence<T> call() throws Exception
                        {
                          // removed eager loading.. especially bad for join query
                          Sequence<T> sequence = query.run(segmentWalker, responseContext);
                          return Sequences.withBaggage(sequence, semaphore);
                        }

                        @Override
                        public String toString()
                        {
                          return query.toString();
                        }
                      };
                    }
                  }
              ), semaphore, priority
          );
          Sequence<Pair<Query<T>, Sequence<T>>> sequence = Sequences.simple(
              GuavaUtils.zip(ready, Lists.transform(futures, FutureSequence.<T>toSequence()))
          );
          return Sequences.withBaggage(
              sequence,
              new Closeable()
              {
                @Override
                public void close() throws IOException
                {
                  semaphore.destroy();
                }
              }
          );
        }
      };
    }

    final PostProcessingOperator<T> postProcessing = PostProcessingOperators.load(this, mapper);

    final QueryRunner<T> runner;
    if (postProcessing != null && postProcessing.supportsUnionProcessing()) {
      runner = ((PostProcessingOperator.UnionSupport<T>) postProcessing).postProcess(baseRunner);
    } else {
      QueryRunner<T> merged = new QueryRunner<T>()
      {
        @Override
        public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
        {
          Sequence<Sequence<T>> sequences = Sequences.map(
              baseRunner.run(query, responseContext), Pair.<Query<T>, Sequence<T>>rhsFn()
          );
          if (isSortOnUnion()) {
            return QueryUtils.mergeSort(query, sequences);
          }
          return Sequences.concat(sequences);
        }
      };
      runner = postProcessing == null ? merged : postProcessing.postProcess(merged);
    }
    if (getLimit() > 0 && getLimit() < Integer.MAX_VALUE) {
      return new QueryRunner<T>()
      {
        @Override
        public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
        {
          return Sequences.limit(query.run(runner, responseContext), getLimit());
        }
      };
    }
    return runner;
  }

  private List<Query<T>> toTargetQueries()
  {
    final List<Query<T>> ready;
    if (queries != null) {
      ready = queries;
    } else {
      final Query<T> target = query;
      ready = Lists.transform(
          target.getDataSource().getNames(), new Function<String, Query<T>>()
          {
            @Override
            public Query<T> apply(String dataSource)
            {
              return target.withDataSource(TableDataSource.of(dataSource));
            }
          }
      );
    }
    return ready;
  }

  @Override
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig, ObjectMapper jsonMapper)
  {
    if (query != null && query.getDataSource() instanceof UnionDataSource) {
      return withQueries(Lists.<Query>newArrayList(Iterables.transform(
          ((UnionDataSource) query.getDataSource()).getDataSources(),
          new Function<TableDataSource, Query<T>>()
          {
            @Override
            public Query<T> apply(TableDataSource input)
            {
              return query.withDataSource(input);
            }
          }
      )));
    }
    return this;
  }
}
