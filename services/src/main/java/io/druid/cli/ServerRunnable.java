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

package io.druid.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Injector;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.guice.GuiceInjectors;
import io.druid.server.Shutdown;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public abstract class ServerRunnable extends GuiceRunnable implements Shutdown.Proc
{
  private static final Logger LOGGER = new Logger(ServerRunnable.class);

  public ServerRunnable(Logger log)
  {
    super(log);
  }

  private volatile Thread runner;
  private volatile Lifecycle lifecycle;

  @Override
  public void run()
  {
    runner = Thread.currentThread();
    try {
      lifecycle = start();
      lifecycle.join();
    }
    catch (Exception e) {
      if (e instanceof InterruptedException) {
        return;
      }
      throw Throwables.propagate(e);
    }
  }

  protected Lifecycle start()
  {
    return initLifecycle(makeInjector());
  }


  @Override
  public void shutdown()
  {
    startShutdown();
  }

  @Override
  public boolean shutdown(long timeout)
  {
    final Thread shutdown = startShutdown();
    if (timeout >= 0) {
      try {
        shutdown.join(timeout);
      }
      catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
    return !shutdown.isAlive();
  }

  private Thread startShutdown()
  {
    final Thread shutdown = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        lifecycle.stop();
        runner.interrupt();
      }
    });
    shutdown.setName("shutdown");
    shutdown.setDaemon(true);
    shutdown.start();
    return shutdown;
  }

  private static final Map<String, Class<? extends ServerRunnable>> COMMANDS =
      ImmutableMap.<String, Class<? extends ServerRunnable>>builder()
                  .put("coordinator", CliCoordinator.class)
                  .put("historical", CliHistorical.class)
                  .put("broker", CliBroker.class)
                  .put("overlord", CliOverlord.class)
                  .put("middleManager", CliMiddleManager.class)
                  .put("realtime", CliRealtime.class)
                  .put("router", CliRouter.class)
                  .build();

  public static void main(String[] args) throws Exception
  {
    // coordinator starts storage module. for derby, it starts derby server in it
    Ordering<String> ordering = Ordering.explicit(
        "zookeeper", "coordinator", "broker", "historical", "overlord", "middleManager", "realtime", "router"
    );
    Arrays.sort(args, ordering);
    List<String> params = Lists.newArrayList(Arrays.asList(args));
    if (params.contains("zookeeper")) {
      Properties startupProperties = new Properties();
      startupProperties.setProperty("clientPort", String.valueOf(2181));
      startupProperties.setProperty("clientPortAddress", "localhost");
      File tempFile = File.createTempFile("zookeeper", "dummy");
      tempFile.delete();
      tempFile.mkdirs();
      startupProperties.setProperty("dataDir", tempFile.getAbsolutePath());

      QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
      try {
        quorumConfiguration.parseProperties(startupProperties);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      final ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
      final ServerConfig configuration = new ServerConfig();
      configuration.readFrom(quorumConfiguration);

      final Thread zookeeper = new Thread()
      {
        public void run()
        {
          try {
            zooKeeperServer.runFromConfig(configuration);
          }
          catch (IOException e) {
            LOGGER.error(e, "ZooKeeper Failed");
          }
        }
      };
      zookeeper.setDaemon(true);
      zookeeper.start();
      zookeeper.join(1000);

      params.remove("zookeeper");
    }

    Lifecycle[] runners = new Lifecycle[params.size()];
    for (int i = 0; i < params.size(); i++) {
      String param = params.get(i);
      int index = param.indexOf(':');
      String command = index < 0 ? param : param.substring(0, index);
      String config = index < 0 ? param : param.substring(index + 1);
      Class<? extends ServerRunnable> clazz = COMMANDS.get(command);
      final ServerRunnable target = clazz.newInstance();
      final Injector injector = GuiceInjectors.makeStartupInjector(
          config + "/runtime.properties"
      );
      injector.injectMembers(target);
      LOGGER.warn("Starting.. %s", param);
      runners[i] = target.start();
    }
    for (Lifecycle thread : runners) {
      thread.join();
    }
  }
}
