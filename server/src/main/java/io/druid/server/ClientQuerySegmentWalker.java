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
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.CachingClusteredClient;
import io.druid.guice.annotations.Processing;
import io.druid.query.FluentQueryRunnerBuilder;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.RetryQueryRunnerConfig;
import io.druid.query.SegmentDescriptor;
import io.druid.query.UnionAllQuery;
import io.druid.query.groupby.GroupByQueryHelper;
import org.joda.time.Interval;

import java.util.concurrent.ExecutorService;

/**
 */
public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final ServiceEmitter emitter;
  private final CachingClusteredClient baseClient;
  private final QueryToolChestWarehouse warehouse;
  private final RetryQueryRunnerConfig retryConfig;
  private final QueryConfig queryConfig;
  private final ObjectMapper objectMapper;
  private final ExecutorService exec;

  @Inject
  public ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      CachingClusteredClient baseClient,
      QueryToolChestWarehouse warehouse,
      RetryQueryRunnerConfig retryConfig,
      QueryConfig queryConfig,
      ObjectMapper objectMapper,
      @Processing ExecutorService exec
  )
  {
    this.emitter = emitter;
    this.baseClient = baseClient;
    this.warehouse = warehouse;
    this.retryConfig = retryConfig;
    this.queryConfig = queryConfig;
    this.objectMapper = objectMapper;
    this.exec = exec;
  }

  @Override
  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
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

    if (query.getDataSource() instanceof QueryDataSource) {
      int maxResult = queryConfig.getMaxResults();
      int maxRowCount = Math.min(
          query.getContextValue(GroupByQueryHelper.CTX_KEY_MAX_RESULTS, maxResult),
          maxResult
      );
      QueryRunner<T> runner = toolChest.handleSubQuery(this, maxRowCount);
      return FluentQueryRunnerBuilder.create(toolChest, runner)
                                     .applyFinalizeResults()
                                     .applyFinalQueryDecoration()
                                     .applyPostProcessingOperator(objectMapper)
                                     .applySubQueryResolver(this)
                                     .build();
    }

    if (query instanceof UnionAllQuery) {
      return ((UnionAllQuery) query).getUnionQueryRunner(objectMapper, exec, this);
    }

    if (query instanceof Query.IteratingQuery) {
      return Queries.makeIteratingQueryRunner((Query.IteratingQuery) query, this);
    }

    return FluentQueryRunnerBuilder.create(toolChest, baseClient)
                                   .applyRetry(retryConfig, objectMapper)
                                   .applyPreMergeDecoration()
                                   .applyMergeResults()
                                   .applyPostMergeDecoration()
                                   .applyFinalizeResults()
                                   .applyFinalQueryDecoration()
                                   .applyPostProcessingOperator(objectMapper)
                                   .emitCPUTimeMetric(emitter)
                                   .build();
  }
}
