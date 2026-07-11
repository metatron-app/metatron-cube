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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.CacheMonitor;
import io.druid.concurrent.Execs;
import io.druid.curator.announcement.Announcer;
import io.druid.guice.CacheModule;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.NodeTypeConfig;
import io.druid.jackson.FunctionModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.DescLookupProvider;
import io.druid.query.ManagementQueryModule;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.lookup.LookupModule;
import io.druid.query.lookup.RemoteLookupProvider;
import io.druid.server.AdminModule;
import io.druid.server.QueryResource;
import io.druid.server.ServiceTypes;
import io.druid.server.coordination.ServerManager;
import io.druid.server.coordination.StandaloneSegmentLoader;
import io.druid.server.http.HistoricalResource;
import io.druid.server.http.SegmentListerResource;
import io.druid.server.initialization.CuratorDiscoveryConfig;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.metrics.MetricsModule;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 * A Historical that runs WITHOUT coordinator, ZooKeeper, or metadata DB. Instead of being assigned segments over
 * ZK by the coordinator, it self-loads them at startup by scanning deep storage ({@link StandaloneSegmentLoader}
 * + a {@code SegmentScanner}), applies the auto residency policy (hot=tmpfs mmap, cold=off-heap range), and serves
 * queries directly at :8083 (no broker needed — see the groupBy direct fix). Configure the scan with
 * {@code druid.standalone.{bucket,baseKey,dataSources}} and set {@code druid.segmentCache.loadMode=auto}.
 *
 * ZK is neutralized in layers: (1) do NOT bind ZkCoordinator/CoordinatorClient — dropping the only synchronous ZK
 * consumers; (2) a non-connecting {@link CuratorFramework} (never started); (3) {@link CuratorDiscoveryConfig}
 * overridden so {@code useDiscovery()} returns false → CoordinatorClient (pulled in transitively by
 * ManagementQueryModule) resolves to NoopServiceDiscovery instead of touching ZK; (4) a no-op {@link Announcer} so
 * LookupModule's lifecycle-managed announcer doesn't call the non-started curator at boot.
 */
@Command(
    name = "standalone",
    description = "Runs a Historical with no coordinator/ZooKeeper/metadata-db — self-loads segments from deep storage"
)
public class CliStandaloneHistorical extends ServerRunnable
{
  private static final Logger log = new Logger(CliStandaloneHistorical.class);

  public CliStandaloneHistorical()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("type")).to(ServiceTypes.HISTORICAL);
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8083);

            binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);
            binder.bind(DescLookupProvider.class).to(RemoteLookupProvider.class);

            LifecycleModule.register(binder, Server.class);
            binder.bind(ServerManager.class).in(LazySingleton.class);
            binder.bind(QuerySegmentWalker.class).to(ServerManager.class).in(LazySingleton.class);

            // self-load from deep storage instead of ZkCoordinator (which is NOT bound here); registering it forces
            // the @LifecycleStart scan+load at boot.
            binder.bind(StandaloneSegmentLoader.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, StandaloneSegmentLoader.class);

            binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig("historical"));
            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
            JsonConfigProvider.bind(binder, "druid.standalone", io.druid.server.coordination.StandaloneCatalogConfig.class);
            Jerseys.addResource(binder, QueryResource.class);
            Jerseys.addResource(binder, HistoricalResource.class);
            Jerseys.addResource(binder, SegmentListerResource.class);
            Jerseys.addResource(binder, io.druid.server.http.DataSourceSchemaResource.class);
            Jerseys.addResource(binder, io.druid.server.http.CatalogResource.class);
            LifecycleModule.register(binder, QueryResource.class);

            // Force NoopServiceDiscovery: CuratorDiscoveryConfig.useDiscovery() is path != null and the path
            // defaults to "/druid/discovery", so we can't get null via a property — override the config so
            // CoordinatorClient (pulled in transitively by ManagementQueryModule) resolves to the no-op selector
            // instead of touching ZK.
            binder.bind(CuratorDiscoveryConfig.class).toInstance(new CuratorDiscoveryConfig()
            {
              @Override
              public boolean useDiscovery()
              {
                return false;
              }
            });

            JsonConfigProvider.bind(binder, "druid.historical.cache", CacheConfig.class);
            binder.install(new CacheModule());
            MetricsModule.register(binder, CacheMonitor.class);
          }

          /** Override CuratorModule's provider with a framework that is never started -> never connects to ZK. */
          @Provides
          @LazySingleton
          public CuratorFramework noZkCurator()
          {
            log.info("standalone: binding a non-connecting CuratorFramework (no ZooKeeper)");
            return CuratorFrameworkFactory.builder()
                                          .connectString("standalone-no-zk:0")
                                          .retryPolicy(new RetryOneTime(1))
                                          .build();   // NOT started
          }

          /**
           * No-op {@link Announcer} so the LookupModule's lifecycle-managed {@code LookupResourceListenerAnnouncer}
           * doesn't touch the non-started curator at boot ({@code Expected state [STARTED] was [LATENT]}). We bind
           * no ZkCoordinator, so this is the ONLY ZK announcer in the injector — neutralizing it is safe.
           */
          @Provides
          @LazySingleton
          public Announcer noopAnnouncer(CuratorFramework curator)
          {
            return new Announcer(curator, Execs.singleThreaded("standalone-noop-announcer-%d"))
            {
              @Override public void start() {}
              @Override public void stop() {}
              @Override public void announce(String path, byte[] bytes) {}
              @Override public void announce(String path, byte[] bytes, boolean removeParentIfCreated) {}
              @Override public void unannounce(String path, boolean shuttingDown) {}
            };
          }
        },
        new LookupModule(),
        new FunctionModule(),
        new ManagementQueryModule(),
        new AdminModule(this)
    );
  }
}
