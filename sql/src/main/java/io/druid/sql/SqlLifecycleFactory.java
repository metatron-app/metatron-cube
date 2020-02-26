/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package io.druid.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.client.BrokerServerView;
import io.druid.guice.LazySingleton;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.AllowAllAuthenticator;
import io.druid.server.security.AuthenticationResult;
import io.druid.sql.calcite.planner.PlannerFactory;

import java.util.Map;

@LazySingleton
public class SqlLifecycleFactory
{
  private final PlannerFactory plannerFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final BrokerServerView brokerServerView;

  @Inject
  public SqlLifecycleFactory(
      PlannerFactory plannerFactory,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      BrokerServerView brokerServerView
  )
  {
    this.plannerFactory = plannerFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.brokerServerView = brokerServerView;
  }

  @VisibleForTesting
  public SqlLifecycle factorize(String sql)
  {
    return factorize(sql, Maps.newHashMap(), AllowAllAuthenticator.ALLOW_ALL_RESULT);
  }

  public SqlLifecycle factorize(String sql, Map<String, Object> context, AuthenticationResult authenticationResult)
  {
    return new SqlLifecycle(
        plannerFactory,
        emitter,
        requestLogger,
        System.currentTimeMillis(),
        System.nanoTime(),
        brokerServerView,
        sql,
        context,
        authenticationResult
    );
  }
}
