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
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;

import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;

import java.util.Map;

/**
 */
public class FinalizeResultsQueryRunner<T> implements QueryRunner<T>
{
  public static <T> QueryRunner<T> finalizeAndPostProcessing(
      final QueryRunner<T> baseRunner,
      final QueryToolChest<T> toolChest,
      final ObjectMapper objectMapper
  )
  {
    return FluentQueryRunnerBuilder.create(toolChest, baseRunner)
                                   .applyFinalizeResults()
                                   .applyFinalQueryDecoration()
                                   .applyPostProcessingOperator()
                                   .build();
  }

  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T> toolChest;

  public FinalizeResultsQueryRunner(QueryRunner<T> baseRunner, QueryToolChest<T> toolChest)
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
  {
    if (query instanceof DelegateQuery) {
      return baseRunner.run(query, responseContext);
    }

    boolean finalize = BaseQuery.isFinalize(query);
    MetricManipulationFn manipulator = finalize ? MetricManipulatorFns.finalizing() : MetricManipulatorFns.identity();

    Function finalizerFn = toolChest.makePostComputeManipulatorFn(query, manipulator);
    if (BaseQuery.isBySegment(query)) {
      finalizerFn = BySegmentResultValue.applyAll(finalizerFn);
    }
    return Sequences.map(baseRunner.run(query, responseContext), finalizerFn);
  }
}
