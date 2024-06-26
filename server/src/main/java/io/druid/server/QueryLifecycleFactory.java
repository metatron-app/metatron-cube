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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.druid.guice.LazySingleton;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.AuthorizerMapper;

@LazySingleton
public class QueryLifecycleFactory
{
  private final QueryManager queryManager;
  private final QueryToolChestWarehouse warehouse;
  private final QuerySegmentWalker segmentWalker;
  private final GenericQueryMetricsFactory metricsFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public QueryLifecycleFactory(
      final QueryManager queryManager,
      final QueryToolChestWarehouse warehouse,
      final QuerySegmentWalker segmentWalker,
      final GenericQueryMetricsFactory metricsFactory,
      final ServiceEmitter emitter,
      final RequestLogger requestLogger,
      final ObjectMapper jsonMapper,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.queryManager = queryManager;
    this.warehouse = warehouse;
    this.segmentWalker = segmentWalker;
    this.metricsFactory = metricsFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.jsonMapper = jsonMapper;
    this.authorizerMapper = authorizerMapper;
  }

  public QueryLifecycle factorize(Query<?> query)
  {
    return new QueryLifecycle(
        query,
        queryManager,
        warehouse,
        segmentWalker,
        metricsFactory,
        emitter,
        requestLogger,
        jsonMapper,
        authorizerMapper,
        System.currentTimeMillis(),
        System.nanoTime()
    );
  }
}
