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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.CachingClusteredClient;
import io.druid.query.FluentQueryRunnerBuilder;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.RetryQueryRunner;
import io.druid.query.RetryQueryRunnerConfig;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.UnionAllQuery;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final ServiceEmitter emitter;
  private final CachingClusteredClient baseClient;
  private final QueryToolChestWarehouse warehouse;
  private final RetryQueryRunnerConfig retryConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      CachingClusteredClient baseClient,
      QueryToolChestWarehouse warehouse,
      RetryQueryRunnerConfig retryConfig,
      ObjectMapper objectMapper
  )
  {
    this.emitter = emitter;
    this.baseClient = baseClient;
    this.warehouse = warehouse;
    this.retryConfig = retryConfig;
    this.objectMapper = objectMapper;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return makeRunner(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeRunner(query);
  }

  @SuppressWarnings("unchecked")
  private <T> QueryRunner<T> makeRunner(Query<T> query)
  {
    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    QueryRunner<T> runner;
    if (query instanceof UnionAllQuery) {
      runner = getUnionQueryRunner((UnionAllQuery) query);
    } else {
      final PostProcessingOperator<T> postProcessing = objectMapper.convertValue(
          query.<String>getContextValue("postProcessing"),
          new TypeReference<PostProcessingOperator<T>>()
          {
          }
      );
      FluentQueryRunnerBuilder<T> builder = new FluentQueryRunnerBuilder<>(toolChest);
      runner = builder.create(new RetryQueryRunner<>(baseClient, toolChest, retryConfig, objectMapper))
                      .applyPreMergeDecoration()
                      .mergeResults()
                      .applyPostMergeDecoration()
                      .emitCPUTimeMetric(emitter)
                      .postProcess(postProcessing);
    }
    return runner;
  }

  private <T extends Comparable<T>> QueryRunner<T> getUnionQueryRunner(final UnionAllQuery<T> union)
  {
    final String queryId = union.getId();
    final boolean sortOnUnion = union.isSortOnUnion();
    final List<Query<T>> targets = union.getQueries();
    if (targets != null) {
      return new QueryRunner<T>()
      {
        @Override
        public Sequence<T> run(Query<T> query, final Map<String, Object> responseContext)
        {
          final Sequence<Sequence<T>> sequences = Sequences.simple(
              Lists.transform(
                  targets,
                  new Function<Query<T>, Sequence<T>>()
                  {
                    @Override
                    public Sequence<T> apply(final Query<T> query)
                    {
                      return new LazySequence<T>(
                          new Supplier<Sequence<T>>()
                          {
                            @Override
                            public Sequence<T> get()
                            {
                              final Query<T> element = query.withId(queryId);
                              return makeRunner(element).run(element, responseContext);
                            }
                          }
                      );
                    }
                  }
              )
          );

          if (sortOnUnion) {
            return new MergeSequence<T>(query.getResultOrdering(), sequences);
          }
          return Sequences.concat(sequences);
        }
      };
    }
    final Query<T> target = union.getQuery().withId(queryId);
    return new QueryRunner<T>()
      {
        @Override
        public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
        {
          final Sequence<Sequence<T>> sequences = Sequences.simple(
              Lists.transform(
                  target.getDataSource().getNames(),
                  new Function<String, Sequence<T>>()
                  {
                    @Override
                    public Sequence<T> apply(final String dataSource)
                    {
                      final Query<T> runner = target.withDataSource(new TableDataSource(dataSource));
                      return new LazySequence<T>(
                          new Supplier<Sequence<T>>()
                          {
                            @Override
                            public Sequence<T> get()
                            {
                              return makeRunner(runner).run(runner, responseContext);
                            }
                          }
                      );
                    }
                  }
              )
          );

          if (sortOnUnion) {
            return new MergeSequence<T>(query.getResultOrdering(), sequences);
          }
          return Sequences.concat(sequences);
        }
      };
  }
}
