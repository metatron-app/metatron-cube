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
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

public class FluentQueryRunnerBuilder<T>
{
  private final QueryToolChest<T, Query<T>> toolChest;
  private final QueryRunner<T> baseRunner;

  public static <T> FluentQueryRunnerBuilder create(QueryToolChest<T, Query<T>> toolChest, QueryRunner<T> baseRunner)
  {
    return new FluentQueryRunnerBuilder<T>(toolChest, baseRunner);
  }

  private FluentQueryRunnerBuilder(QueryToolChest<T, Query<T>> toolChest, QueryRunner<T> baseRunner)
  {
    this.toolChest = toolChest;
    this.baseRunner = baseRunner;
  }

  public QueryRunner<T> build()
  {
    return baseRunner;
  }

  public FluentQueryRunnerBuilder from(QueryRunner<T> runner)
  {
    return new FluentQueryRunnerBuilder<T>(toolChest, runner);
  }

  public FluentQueryRunnerBuilder applyPreMergeDecoration()
  {
    return from(new UnionQueryRunner<T>(toolChest.preMergeQueryDecoration(baseRunner)));
  }

  public FluentQueryRunnerBuilder applyMergeResults()
  {
    return from(toolChest.mergeResults(baseRunner));
  }

  public FluentQueryRunnerBuilder applyPostMergeDecoration()
  {
    return from(toolChest.postMergeQueryDecoration(baseRunner));
  }

  public FluentQueryRunnerBuilder applyFinalizeResults()
  {
    return from(toolChest.finalizeResults(baseRunner));
  }

  @SuppressWarnings("unchecked")
  public FluentQueryRunnerBuilder applyFinalQueryDecoration()
  {
    return from(toolChest.finalQueryDecoration(baseRunner));
  }

  public FluentQueryRunnerBuilder applyPostProcessingOperator(ObjectMapper mapper)
  {
    return from(PostProcessingOperators.wrap(baseRunner, mapper));
  }

  public FluentQueryRunnerBuilder emitCPUTimeMetric(ServiceEmitter emitter)
  {
    return from(
        CPUTimeMetricQueryRunner.safeBuild(
            baseRunner,
            new Function<Query<T>, ServiceMetricEvent.Builder>()
            {
              @Nullable
              @Override
              public ServiceMetricEvent.Builder apply(Query<T> tQuery)
              {
                return toolChest.makeMetricBuilder(tQuery);
              }
            },
            emitter,
            new AtomicLong(0L),
            true
        )
    );
  }
}
