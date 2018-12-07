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
import com.google.common.collect.ImmutableMap;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;

import java.util.Map;

/**
 */
public class FinalizeResultsQueryRunner<T> implements QueryRunner<T>
{
  public static <T> QueryRunner<T> finalize(
      final QueryRunner<T> baseRunner,
      final QueryToolChest<T, Query<T>> toolChest,
      final ObjectMapper objectMapper
  )
  {
    return new QueryRunner<T>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        QueryRunner<T> runner = PostProcessingOperators.wrap(
            toolChest.finalQueryDecoration(toolChest.finalizeResults(baseRunner)), objectMapper
        );
        return runner.run(query, responseContext);
      }
    };
  }

  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;

  public FinalizeResultsQueryRunner(QueryRunner<T> baseRunner, QueryToolChest<T, Query<T>> toolChest)
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

    final Query<T> queryToRun;
    final MetricManipulationFn metricManipulationFn;

    if (BaseQuery.getContextFinalize(query, true)) {
      queryToRun = query.withOverriddenContext(ImmutableMap.<String, Object>of(QueryContextKeys.FINALIZE, false));
      metricManipulationFn = MetricManipulatorFns.finalizing();
    } else {
      queryToRun = query;
      metricManipulationFn = MetricManipulatorFns.identity();
    }

    Function finalizerFn = toolChest.makePostComputeManipulatorFn(query, metricManipulationFn);
    if (BaseQuery.getContextBySegment(query)) {
      finalizerFn = BySegmentResultValueClass.applyAll(finalizerFn);
    }
    return Sequences.map(baseRunner.run(queryToRun, responseContext), finalizerFn);
  }
}
