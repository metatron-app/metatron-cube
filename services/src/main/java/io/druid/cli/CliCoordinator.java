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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.logger.Logger;
import io.airlift.airline.Command;
import io.druid.audit.AuditManager;
import io.druid.client.CoordinatorServerView;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.guice.ConditionalMultibind;
import io.druid.guice.ConfigProvider;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.CoordinatorIndexingServiceHelper;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataRuleManagerConfig;
import io.druid.metadata.MetadataRuleManagerProvider;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.metadata.MetadataSegmentManagerConfig;
import io.druid.metadata.MetadataSegmentManagerProvider;
import io.druid.metadata.MetadataStorage;
import io.druid.metadata.MetadataStorageProvider;
import io.druid.server.AdminModule;
import io.druid.server.ServiceTypes;
import io.druid.server.audit.AuditManagerProvider;
import io.druid.server.coordinator.BalancerStrategyFactory;
import io.druid.server.coordinator.CoordinatorDynamicConfig;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorCleanupPendingSegments;
import io.druid.server.coordinator.DruidCoordinatorConfig;
import io.druid.server.coordinator.LoadQueueTaskMaster;
import io.druid.server.coordinator.helper.DruidCoordinatorHelper;
import io.druid.server.coordinator.helper.DruidCoordinatorSegmentKiller;
import io.druid.server.coordinator.helper.DruidCoordinatorSegmentMerger;
import io.druid.server.coordinator.helper.DruidCoordinatorVersionConverter;
import io.druid.server.http.CoordinatorCompactionConfigsResource;
import io.druid.server.http.CoordinatorDynamicConfigsResource;
import io.druid.server.http.CoordinatorRedirectInfo;
import io.druid.server.http.CoordinatorResource;
import io.druid.server.http.DataSourcesResource;
import io.druid.server.http.IntervalsResource;
import io.druid.server.http.LookupCoordinatorResource;
import io.druid.server.http.MetadataResource;
import io.druid.server.http.RedirectFilter;
import io.druid.server.http.RedirectInfo;
import io.druid.server.http.RulesResource;
import io.druid.server.http.ServersResource;
import io.druid.server.http.TiersResource;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.listener.announcer.ListenerDiscoverer;
import io.druid.server.lookup.cache.LookupCoordinatorManager;
import io.druid.server.lookup.cache.LookupCoordinatorManagerConfig;
import org.apache.curator.framework.CuratorFramework;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 */
@Command(
    name = "coordinator",
    description = "Runs the Coordinator, see http://druid.io/docs/latest/Coordinator.html for a description."
)
public class CliCoordinator extends ServerRunnable
{
  private static final Logger log = new Logger(CliCoordinator.class);

  private Properties properties;

  public CliCoordinator()
  {
    super(log);
  }

  @Inject
  public void configure(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant()
                  .annotatedWith(Names.named("type")).to(ServiceTypes.COORDINATOR);
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8081);

            ConfigProvider.bind(binder, DruidCoordinatorConfig.class);

            binder.bind(MetadataStorage.class)
                  .toProvider(MetadataStorageProvider.class)
                  .in(ManageLifecycle.class);

            JsonConfigProvider.bind(binder, "druid.manager.segments", MetadataSegmentManagerConfig.class);
            JsonConfigProvider.bind(binder, "druid.manager.rules", MetadataRuleManagerConfig.class);
            JsonConfigProvider.bind(binder, "druid.manager.lookups", LookupCoordinatorManagerConfig.class);
            JsonConfigProvider.bind(
                binder,
                "druid.coordinator.dynamic",
                CoordinatorDynamicConfig.class,
                new CoordinatorDynamicConfig()
            );
            JsonConfigProvider.bind(binder, "druid.coordinator.balancer", BalancerStrategyFactory.class);

            binder.bind(RedirectFilter.class).in(LazySingleton.class);
            binder.bind(RedirectInfo.class).to(CoordinatorRedirectInfo.class).in(LazySingleton.class);

            binder.bind(MetadataSegmentManager.class)
                  .toProvider(MetadataSegmentManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(MetadataRuleManager.class)
                  .toProvider(MetadataRuleManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(AuditManager.class)
                  .toProvider(AuditManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(IndexingServiceClient.class).in(LazySingleton.class);
            binder.bind(CoordinatorServerView.class).in(LazySingleton.class);

            binder.bind(DruidCoordinator.class);

            binder.bind(LookupCoordinatorManager.class).in(ManageLifecycle.class);
            binder.bind(ListenerDiscoverer.class).in(ManageLifecycle.class);

            LifecycleModule.register(binder, ListenerDiscoverer.class);
            LifecycleModule.register(binder, MetadataStorage.class);
            LifecycleModule.register(binder, DruidCoordinator.class);
            LifecycleModule.register(binder, LookupCoordinatorManager.class);

            binder.bind(JettyServerInitializer.class)
                  .to(CoordinatorJettyServerInitializer.class);

            Jerseys.addResource(binder, CoordinatorResource.class);
            Jerseys.addResource(binder, CoordinatorDynamicConfigsResource.class);
            Jerseys.addResource(binder, CoordinatorCompactionConfigsResource.class);
            Jerseys.addResource(binder, TiersResource.class);
            Jerseys.addResource(binder, RulesResource.class);
            Jerseys.addResource(binder, ServersResource.class);
            Jerseys.addResource(binder, DataSourcesResource.class);
            Jerseys.addResource(binder, MetadataResource.class);
            Jerseys.addResource(binder, IntervalsResource.class);
            Jerseys.addResource(binder, LookupCoordinatorResource.class);

            LifecycleModule.register(binder, Server.class);
            LifecycleModule.register(binder, DataSourcesResource.class);

            ConditionalMultibind.create(
                properties,
                binder,
                DruidCoordinatorHelper.class,
                CoordinatorIndexingServiceHelper.class
            ).addConditionBinding(
                "druid.coordinator.merge.on",
                Predicates.equalTo("true"),
                DruidCoordinatorSegmentMerger.class
            ).addConditionBinding(
                "druid.coordinator.conversion.on",
                Predicates.equalTo("true"),
                DruidCoordinatorVersionConverter.class
            ).addConditionBinding(
                "druid.coordinator.kill.on",
                Predicates.equalTo("true"),
                DruidCoordinatorSegmentKiller.class
            ).addConditionBinding(
                    "druid.coordinator.kill.pendingSegments.on",
                    Predicates.equalTo("true"),
                    DruidCoordinatorCleanupPendingSegments.class
            );

          }

          @Provides
          @LazySingleton
          public LoadQueueTaskMaster getLoadQueueTaskMaster(
              CuratorFramework curator,
              ObjectMapper jsonMapper,
              ScheduledExecutorFactory factory,
              DruidCoordinatorConfig config
          )
          {
            return new LoadQueueTaskMaster(
                curator, jsonMapper, factory.create(1, "Master-PeonExec--%d"),
                Executors.newSingleThreadExecutor(), config
            );
          }
        },
        new AdminModule(this)
    );
  }
}
