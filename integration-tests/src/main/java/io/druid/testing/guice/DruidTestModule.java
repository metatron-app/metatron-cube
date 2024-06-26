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

package io.druid.testing.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.java.util.emitter.core.LoggingEmitter;
import io.druid.java.util.emitter.core.LoggingEmitterConfig;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.curator.CuratorConfig;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.IntegrationTestingConfigProvider;
import io.druid.testing.IntegrationTestingCuratorConfig;

/**
 */
public class DruidTestModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(IntegrationTestingConfig.class).toProvider(IntegrationTestingConfigProvider.class).in(ManageLifecycle.class);
    JsonConfigProvider.bind(binder, "druid.test.config", IntegrationTestingConfigProvider.class);

    binder.bind(CuratorConfig.class).to(IntegrationTestingCuratorConfig.class);
  }

  @Provides
  @ManageLifecycle
  public ServiceEmitter getServiceEmitter(Supplier<LoggingEmitterConfig> config, ObjectMapper jsonMapper)
  {
    return new ServiceEmitter("", "", new LoggingEmitter(config.get(), jsonMapper));
  }
}
