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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.CachingClusteredClient;
import io.druid.common.guava.FutureSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.Processing;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.BaseQuery;
import io.druid.query.FluentQueryRunnerBuilder;
import io.druid.query.PostProcessingOperator;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.RetryQueryRunner;
import io.druid.query.RetryQueryRunnerConfig;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.UnionAllQuery;
import io.druid.query.UnionAllQueryRunner;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryHelper;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 */
public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private static final Logger LOG = new Logger(ClientQuerySegmentWalker.class);

  private final ServiceEmitter emitter;
  private final CachingClusteredClient baseClient;
  private final QueryToolChestWarehouse warehouse;
  private final RetryQueryRunnerConfig retryConfig;
  private final GroupByQueryConfig groupByConfig;
  private final ObjectMapper objectMapper;
  private final ExecutorService exec;

  @Inject
  public ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      CachingClusteredClient baseClient,
      QueryToolChestWarehouse warehouse,
      RetryQueryRunnerConfig retryConfig,
      GroupByQueryConfig groupByConfig,
      ObjectMapper objectMapper,
      @Processing ExecutorService exec
  )
  {
    this.emitter = emitter;
    this.baseClient = baseClient;
    this.warehouse = warehouse;
    this.retryConfig = retryConfig;
    this.groupByConfig = groupByConfig;
    this.objectMapper = objectMapper;
    this.exec = exec;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return makeRunner(query, false);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeRunner(query, false);
  }

  @SuppressWarnings("unchecked")
  private <T> QueryRunner<T> makeRunner(Query<T> query, boolean subQuery)
  {
    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    if (query.getDataSource() instanceof QueryDataSource) {
      Query innerQuery = ((QueryDataSource)query.getDataSource()).getQuery().withOverriddenContext(query.getContext());
      int maxResult = groupByConfig.getMaxResults();
      int maxRowCount = Math.min(
          query.getContextValue(GroupByQueryHelper.CTX_KEY_MAX_RESULTS, maxResult),
          maxResult
      );
      QueryRunner runner = toolChest.handleSubQuery(makeRunner(innerQuery, true), this, exec, maxRowCount);
      PostProcessingOperator postProcessing = PostProcessingOperators.load(query, objectMapper);
      return toolChest.finalQueryDecoration(
          postProcessing != null ? postProcessing.postProcess(runner) : runner
      );
    }

    if (query instanceof UnionAllQuery) {
      return getUnionQueryRunner((UnionAllQuery) query, objectMapper);
    }

    if (query instanceof Query.IteratingQuery) {
      return getIteratingQueryRunner((Query.IteratingQuery) query);
    }

    FluentQueryRunnerBuilder<T> builder = new FluentQueryRunnerBuilder<>(toolChest);
    FluentQueryRunnerBuilder.FluentQueryRunner runner = builder.create(
        new RetryQueryRunner<>(baseClient, toolChest, retryConfig, objectMapper)
    );

    runner = runner.applyPreMergeDecoration()
                   .mergeResults()
                   .applyPostMergeDecoration();
    if (!subQuery) {
      runner = runner.applyFinalizeResults()
                     .emitCPUTimeMetric(emitter);
    }
    return runner.postProcess(PostProcessingOperators.load(query, objectMapper));
  }

  private <I, T> QueryRunner<T> getIteratingQueryRunner(final Query.IteratingQuery<I, T> iterating)
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
                    Sequence<I> sequence = makeRunner(query, false).run(query, responseContext);
                    Pair<Sequence<T>, Query<I>> next = iterating.next(sequence, query);
                    query = next.rhs;
                    return next.lhs;
                  }
                };
              }
            }
        );
      }
    };
  }

  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> QueryRunner<T> getUnionQueryRunner(
      final UnionAllQuery<T> union,
      final ObjectMapper mapper
  )
  {
    final String queryId = union.getId();
    final boolean sortOnUnion = union.isSortOnUnion();
    final PostProcessingOperator<T> postProcessing = PostProcessingOperators.load(union, mapper);

    final UnionAllQueryRunner<T> baseRunner;
    if (union.getParallelism() <= 1) {
     // executes when the first element of the sequence is accessed
     baseRunner = new UnionAllQueryRunner<T>()
      {
        @Override
        public Sequence<Pair<Query<T>, Sequence<T>>> run(final Query<T> query, final Map<String, Object> responseContext)
        {
          final List<Query<T>> ready = toTargetQueries((UnionAllQuery<T>) query, queryId);
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
                                  return makeRunner(query, false).run(query, responseContext);
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
        final int priority = BaseQuery.getContextPriority(union, 0);
        @Override
        public Sequence<Pair<Query<T>, Sequence<T>>> run(final Query<T> query, final Map<String, Object> responseContext)
        {
          final List<Query<T>> ready = toTargetQueries((UnionAllQuery<T>) query, queryId);
          final int parallelism = Math.min(union.getParallelism(), ready.size());
          final Execs.Semaphore semaphore = new Execs.Semaphore(Math.max(parallelism, union.getQueue()));
          LOG.info("Starting %d parallel works with %d threads", ready.size(), parallelism);
          final List<ListenableFuture<Sequence<T>>> futures = Execs.execute(
              exec, Lists.transform(
                  ready, new Function<Query<T>, Callable<Sequence<T>>>()
                  {
                    @Override
                    public Callable<Sequence<T>> apply(final Query<T> query)
                    {
                      return new AbstractPrioritizedCallable<Sequence<T>>(priority)
                      {
                        @Override
                        public Sequence<T> call() throws Exception
                        {
                          // should eagerly retrieve result in executor
                          Sequence<T> sequence = makeRunner(query, false).run(query, responseContext);
                          sequence = Sequences.simple(Sequences.toList(sequence, Lists.<T>newArrayList()));
                          return new ResourceClosingSequence<T>(sequence, semaphore);
                        }

                        @Override
                        public String toString()
                        {
                          return query.toString();
                        }
                      };
                    }
                  }
              ), semaphore, parallelism, priority
          );
          Sequence<Pair<Query<T>, Sequence<T>>> sequence = Sequences.simple(
              GuavaUtils.zip(ready, Lists.transform(futures, FutureSequence.<T>toSequence()))
          );
          return new ResourceClosingSequence<Pair<Query<T>, Sequence<T>>>(
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
          if (sortOnUnion) {
            return new MergeSequence<T>(query.getResultOrdering(), sequences);
          }
          return Sequences.concat(sequences);
        }
      };
      runner = postProcessing == null ? merged : postProcessing.postProcess(merged);
    }
    if (union.getLimit() > 0 && union.getLimit() < Integer.MAX_VALUE) {
      return new QueryRunner<T>()
      {
        @Override
        public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
        {
          return Sequences.limit(query.run(runner, responseContext), union.getLimit());
        }
      };
    }
    return runner;
  }

  private <T extends Comparable<T>> List<Query<T>> toTargetQueries(UnionAllQuery<T> union, final String queryId)
  {
    final List<Query<T>> ready;
    if (union.getQueries() != null) {
      ready = Lists.transform(
          union.getQueries(), new Function<Query<T>, Query<T>>()
          {
            @Override
            public Query<T> apply(Query<T> query)
            {
              return query.withId(queryId);
            }
          }
      );
    } else {
      final Query<T> target = union.getQuery().withId(queryId);
      ready = Lists.transform(
          target.getDataSource().getNames(), new Function<String, Query<T>>()
          {
            @Override
            public Query<T> apply(String dataSource)
            {
              return target.withDataSource(new TableDataSource(dataSource));
            }
          }
      );
    }
    return ready;
  }
}
