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

package io.druid.query.jmx;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.query.DruidMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.aggregation.MetricManipulationFn;

import java.util.Map;

/**
 */
public class JMXQueryToolChest extends QueryToolChest<Map<String, Object>, JMXQuery>
{
  private static final TypeReference<Map<String, Object>> TYPE_REFERENCE =
      new TypeReference<Map<String, Object>>()
      {
      };

  @Override
  public QueryRunner<Map<String, Object>> mergeResults(QueryRunner<Map<String, Object>> queryRunner)
  {
    return queryRunner;
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(JMXQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<Map<String, Object>, Map<String, Object>> makePreComputeManipulatorFn(
      final JMXQuery query, final MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Map<String, Object>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }
}