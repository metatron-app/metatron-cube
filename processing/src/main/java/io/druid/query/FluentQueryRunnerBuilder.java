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
import io.druid.java.util.emitter.service.ServiceEmitter;

import java.util.concurrent.atomic.AtomicLong;

public class FluentQueryRunnerBuilder<T>
{
  private final QueryToolChest<T> toolChest;
  private final QueryRunner<T> runner;

  public static <T> FluentQueryRunnerBuilder<T> create(QueryToolChest<T> toolChest, QueryRunner<T> baseRunner)
  {
    return new FluentQueryRunnerBuilder<T>(toolChest, baseRunner);
  }

  private FluentQueryRunnerBuilder(QueryToolChest<T> toolChest, QueryRunner<T> baseRunner)
  {
    this.toolChest = toolChest;
    this.runner = baseRunner;
  }

  public QueryRunner<T> build()
  {
    return runner;
  }

  public FluentQueryRunnerBuilder<T> from(QueryRunner<T> runner)
  {
    return new FluentQueryRunnerBuilder<T>(toolChest, runner);
  }

  public FluentQueryRunnerBuilder<T> applyRetry(RetryQueryRunnerConfig config, ObjectMapper jsonMapper)
  {
    return from(new RetryQueryRunner<T>(runner, config, jsonMapper));
  }

  public FluentQueryRunnerBuilder<T> applyPreMergeDecoration()
  {
    return from(new UnionQueryRunner<T>(toolChest == null ? runner : toolChest.preMergeQueryDecoration(runner)));
  }

  public FluentQueryRunnerBuilder<T> applyMergeResults()
  {
    return from(toolChest.mergeResults(runner));
  }

  public FluentQueryRunnerBuilder<T> applyPostMergeDecoration()
  {
    return toolChest == null ? this : from(toolChest.postMergeQueryDecoration(runner));
  }

  public FluentQueryRunnerBuilder<T> applyFinalizeResults()
  {
    return toolChest == null ? this : from(toolChest.finalizeResults(runner));
  }

  public FluentQueryRunnerBuilder<T> applyFinalQueryDecoration()
  {
    return toolChest == null ? this : from(toolChest.finalQueryDecoration(runner));
  }

  public FluentQueryRunnerBuilder<T> applyPostProcessingOperator()
  {
    return from(PostProcessingOperators.wrap(runner));
  }

  public FluentQueryRunnerBuilder<T> applySubQueryResolver(QuerySegmentWalker segmentWalker)
  {
    return from(QueryRunners.getSubQueryResolver(runner, toolChest, segmentWalker));
  }

  public FluentQueryRunnerBuilder<T> runWithLocalized(QuerySegmentWalker segmentWalker)
  {
    return from(QueryRunners.<T>runWithLocalized(runner, segmentWalker));
  }

  public FluentQueryRunnerBuilder<T> runWith(Query<T> query)
  {
    return from(QueryRunners.<T>runWith(query, runner));
  }

  public FluentQueryRunnerBuilder<T> emitCPUTimeMetric(ServiceEmitter emitter)
  {
    return toolChest == null ? this : from(
        CPUTimeMetricQueryRunner.safeBuild(
            runner,
            toolChest,
            emitter,
            new AtomicLong(0L),
            true
        )
    );
  }
}
