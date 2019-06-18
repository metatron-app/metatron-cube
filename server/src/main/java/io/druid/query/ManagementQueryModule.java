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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.multibindings.MapBinder;
import io.druid.guice.LazySingleton;
import io.druid.guice.QueryToolBinders;
import io.druid.initialization.DruidModule;
import io.druid.query.config.ConfigQuery;
import io.druid.query.config.ConfigQueryRunnerFactory;
import io.druid.query.config.ConfigQueryToolChest;
import io.druid.query.jmx.JMXQuery;
import io.druid.query.jmx.JMXQueryRunnerFactory;
import io.druid.query.jmx.JMXQueryToolChest;

import java.util.Arrays;
import java.util.List;

public class ManagementQueryModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    if (binder != null) {
      // binder == null for tests
      MapBinder<Class<? extends Query>, QueryToolChest> toolChests = QueryToolBinders.queryToolChestBinder(binder);
      toolChests.addBinding(JMXQuery.class).to(JMXQueryToolChest.class);
      binder.bind(JMXQueryToolChest.class).in(LazySingleton.class);
      toolChests.addBinding(ConfigQuery.class).to(ConfigQueryToolChest.class);
      binder.bind(ConfigQueryToolChest.class).in(LazySingleton.class);

      MapBinder<Class<? extends Query>, QueryRunnerFactory> factories = QueryToolBinders.queryRunnerFactoryBinder(binder);
      factories.addBinding(JMXQuery.class).to(JMXQueryRunnerFactory.class);
      binder.bind(JMXQueryRunnerFactory.class).in(LazySingleton.class);
      factories.addBinding(ConfigQuery.class).to(ConfigQueryRunnerFactory.class);
      binder.bind(ConfigQueryRunnerFactory.class).in(LazySingleton.class);
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.asList(
        new SimpleModule("ManagementModule")
            .registerSubtypes(JMXQuery.class)
            .registerSubtypes(ConfigQuery.class)
    );
  }
}
