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

import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Processing;

import java.util.concurrent.ExecutorService;

public class IntervalChunkingQueryRunnerDecorator
{
  private final ExecutorService executor;
  private final QueryWatcher queryWatcher;
  private final ServiceEmitter emitter;

  @Inject
  public IntervalChunkingQueryRunnerDecorator(@Processing ExecutorService executor, QueryWatcher queryWatcher,
      ServiceEmitter emitter)
  {
    this.executor = executor;
    this.queryWatcher = queryWatcher;
    this.emitter = emitter;
  }

  public <T> QueryRunner<T> decorate(QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest)
  {
    return new IntervalChunkingQueryRunner<T>(delegate, (QueryToolChest<T, Query<T>>)toolChest,
        executor, queryWatcher, emitter);
  }
}
