/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Function;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.concurrent.atomic.AtomicLong;

public class CPUTimeMetricBuilder<T> implements Function<Query<T>, ServiceMetricEvent.Builder>
{
  private final Function<Query<T>, ServiceMetricEvent.Builder> builderFn;
  private final ServiceEmitter emitter;
  private final AtomicLong accumulator;

  public CPUTimeMetricBuilder(
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      ServiceEmitter emitter
  )
  {
    this.builderFn = builderFn;
    this.emitter = emitter;
    this.accumulator = new AtomicLong();
  }

  @Override
  public ServiceMetricEvent.Builder apply(Query<T> input)
  {
    return builderFn.apply(input);
  }

  public QueryRunner<T> accumulate(QueryRunner<T> runner)
  {
    return CPUTimeMetricQueryRunner.safeBuild(runner, builderFn, emitter, accumulator, false);
  }

  public QueryRunner<T> report(QueryRunner<T> runner)
  {
    return CPUTimeMetricQueryRunner.safeBuild(runner, builderFn, emitter, accumulator, true);
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public AtomicLong getAccumulator()
  {
    return accumulator;
  }
}
