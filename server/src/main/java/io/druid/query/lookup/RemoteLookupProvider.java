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

package io.druid.query.lookup;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.guice.annotations.Processing;
import io.druid.math.expr.Function;
import io.druid.math.expr.Functions;
import io.druid.metadata.DescExtractor;
import io.druid.metadata.DescLookupProvider;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class RemoteLookupProvider implements DescLookupProvider
{
  private final ExecutorService exec;
  private final CoordinatorClient coordinator;
  private final Map<String, Object> cached;

  @Inject
  public RemoteLookupProvider(@Processing ExecutorService exec, CoordinatorClient coordinator)
  {
    this.exec = exec;
    this.coordinator = coordinator;
    this.cached = Maps.newHashMap();
  }

  @Override
  public Function.Factory init(String dataSource, DescExtractor extractor)
  {
    Object mapping = cached.get(dataSource);
    if (mapping == null) {
      mapping = coordinator.fetchTableDesc(dataSource, extractor.name(), extractor.getReturnType());
      if (mapping == null) {
        return Functions.NOT_FOUND(extractor.functionName());
      }
      cached.put(dataSource, mapping);
    }
    return extractor.toFunction(mapping);
  }

  public Future<Function.Factory> prepare(final String dataSource, final DescExtractor extractor)
  {
    Object mapping = cached.get(dataSource);
    if (mapping != null) {
      return Futures.immediateFuture(extractor.toFunction(mapping));
    }
    return exec.submit(
        new Callable<Function.Factory>()
        {
          @Override
          public Function.Factory call() throws Exception
          {
            return init(dataSource, extractor);
          }
        }
    );
  }
}
