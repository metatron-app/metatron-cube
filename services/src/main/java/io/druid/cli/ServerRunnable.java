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

package io.druid.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.guice.GuiceInjectors;

import java.util.Map;

/**
 */
public abstract class ServerRunnable extends GuiceRunnable
{
  private static final Logger LOGGER = new Logger(ServerRunnable.class);

  public ServerRunnable(Logger log)
  {
    super(log);
  }

  @Override
  public void run()
  {
    try {
      start().join();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected Lifecycle start()
  {
    return initLifecycle(makeInjector());
  }

  private static final Map<String, Class<? extends ServerRunnable>> COMMANDS =
      ImmutableMap.<String, Class<? extends ServerRunnable>>builder()
                  .put("coordinator", CliCoordinator.class)
                  .put("historical", CliHistorical.class)
                  .put("broker", CliBroker.class)
                  .put("overlord", CliOverlord.class)
                  .put("middleManager", CliMiddleManager.class)
                  .put("realtime", CliRealtime.class).build();

  public static void main(String[] args) throws Exception
  {
    Lifecycle[] runners = new Lifecycle[args.length];
    for (int i = 0; i < args.length; i++) {
      Class<? extends ServerRunnable> clazz = COMMANDS.get(args[i]);
      final ServerRunnable target = clazz.newInstance();
      final Injector injector = GuiceInjectors.makeStartupInjector(
          args[i] + "/runtime.properties"
      );
      injector.injectMembers(target);
      LOGGER.info("Starting "  + args[i]);
      runners[i] = target.start();
    }
    for (Lifecycle thread : runners) {
      thread.join();
    }
  }
}
