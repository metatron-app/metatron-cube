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
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.druid.cli.shell.DruidShell;
import io.druid.guice.IndexingServiceModuleHelper;

import java.util.List;

/**
 */
@Command(name = "shell", description = "Runs the shell.")
public class SimpleShell extends GuiceRunnable
{
  private static final Logger log = new Logger(SimpleShell.class);

  public SimpleShell()
  {
    super(log);
  }

  @Arguments(description = "Additional arguments")
  public List<String> args;

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/shell");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
            IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);
            binder.bind(DruidShell.class);
          }
        }
    );
  }

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final DruidShell shell = injector.getInstance(DruidShell.class);
    final Lifecycle lifeCycle = initLifecycle(injector);
    try {
      shell.run(args);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      lifeCycle.stop();
    }
  }
}
