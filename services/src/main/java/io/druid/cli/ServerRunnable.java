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
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.guice.GuiceInjectors;
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
      zookeeper.start();

      params.remove("zookeeper");
    }

    Lifecycle[] runners = new Lifecycle[params.size()];
    for (int i = 0; i < params.size(); i++) {
      Class<? extends ServerRunnable> clazz = COMMANDS.get(params.get(i));
      final ServerRunnable target = clazz.newInstance();
      final Injector injector = GuiceInjectors.makeStartupInjector(
          params.get(i) + "/runtime.properties"
      );
      injector.injectMembers(target);
      LOGGER.info("Starting "  + params.get(i));
      runners[i] = target.start();
    }
    for (Lifecycle thread : runners) {
      thread.join();
    }
  }
}
