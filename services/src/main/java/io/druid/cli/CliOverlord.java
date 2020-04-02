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
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.google.inject.servlet.DelegatedGuiceFilter;
import com.google.inject.util.Providers;
import io.airlift.airline.Command;
import io.druid.audit.AuditManager;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.guice.IndexingServiceFirehoseModule;
import io.druid.guice.IndexingServiceModuleHelper;
import io.druid.guice.IndexingServiceTaskLogsModule;
import io.druid.guice.JacksonConfigProvider;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ListProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.PolyBind;
import io.druid.guice.annotations.Json;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.tasklogs.SwitchingTaskLogStreamer;
import io.druid.indexing.common.tasklogs.TaskRunnerTaskLogStreamer;
import io.druid.indexing.overlord.ForkingTaskRunnerFactory;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import io.druid.indexing.overlord.MetadataTaskStorage;
import io.druid.indexing.overlord.RemoteTaskRunnerFactory;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunnerFactory;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerResourceManagementConfig;
import io.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerResourceManagementStrategy;
import io.druid.indexing.overlord.autoscaling.ResourceManagementSchedulerConfig;
import io.druid.indexing.overlord.autoscaling.ResourceManagementStrategy;
import io.druid.indexing.overlord.autoscaling.SimpleWorkerResourceManagementConfig;
import io.druid.indexing.overlord.autoscaling.SimpleWorkerResourceManagementStrategy;
import io.druid.indexing.overlord.config.TaskQueueConfig;
import io.druid.indexing.overlord.helpers.OverlordHelper;
import io.druid.indexing.overlord.helpers.TaskLogAutoCleaner;
import io.druid.indexing.overlord.helpers.TaskLogAutoCleanerConfig;
import io.druid.indexing.overlord.http.OverlordRedirectInfo;
import io.druid.indexing.overlord.http.OverlordResource;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.indexing.overlord.supervisor.SupervisorResource;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.server.AdminModule;
import io.druid.server.GuiceServletConfig;
import io.druid.server.ServiceTypes;
import io.druid.server.audit.AuditManagerProvider;
import io.druid.server.http.RedirectFilter;
import io.druid.server.http.RedirectInfo;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationUtils;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import io.druid.tasklogs.TaskLogStreamer;
import io.druid.tasklogs.TaskLogs;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceCollection;

import java.util.List;

/**
 */
@Command(
    name = "overlord",
    description = "Runs an Overlord node, see http://druid.io/docs/latest/Indexing-Service.html for a description"
)
public class CliOverlord extends ServerRunnable
{
  private static Logger log = new Logger(CliOverlord.class);

  static final List<String> UNSECURED_PATHS = ImmutableList.of(
      "/druid/indexer/v1/isLeader",
      "/status/health"
  );

  public CliOverlord()
  {
    super(log);
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
                  .annotatedWith(Names.named("type")).to(ServiceTypes.OVERLORD);
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8090);

            JsonConfigProvider.bind(binder, "druid.indexer.queue", TaskQueueConfig.class);
            JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);

            binder.bind(TaskMaster.class).in(ManageLifecycle.class);

            binder.bind(TaskLogStreamer.class).to(SwitchingTaskLogStreamer.class).in(LazySingleton.class);
            binder.bind(
                new TypeLiteral<List<TaskLogStreamer>>()
                {
                }
            )
                  .toProvider(
                      new ListProvider<TaskLogStreamer>()
                          .add(TaskRunnerTaskLogStreamer.class)
                          .add(TaskLogs.class)
                  )
                  .in(LazySingleton.class);

            binder.bind(TaskActionClientFactory.class).to(LocalTaskActionClientFactory.class).in(LazySingleton.class);
            binder.bind(TaskActionToolbox.class).in(LazySingleton.class);
            binder.bind(TaskLockbox.class).in(LazySingleton.class);
            binder.bind(CoordinatorClient.class).in(LazySingleton.class);
            binder.bind(TaskStorageQueryAdapter.class).in(LazySingleton.class);
            binder.bind(IndexerMetadataStorageAdapter.class).in(LazySingleton.class);
            binder.bind(SupervisorManager.class).in(LazySingleton.class);

            binder.bind(ChatHandlerProvider.class).toProvider(Providers.<ChatHandlerProvider>of(null));

            configureTaskStorage(binder);
            configureAutoscale(binder);
            configureRunners(binder);
            configureOverlordHelpers(binder);

            binder.bind(AuditManager.class)
                  .toProvider(AuditManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(RedirectFilter.class).in(LazySingleton.class);
            binder.bind(RedirectInfo.class).to(OverlordRedirectInfo.class).in(LazySingleton.class);
            binder.bind(JettyServerInitializer.class)
                  .to(OverlordJettyServerInitializer.class)
                  .in(LazySingleton.class);

            Jerseys.addResource(binder, OverlordResource.class);
            Jerseys.addResource(binder, SupervisorResource.class);

            LifecycleModule.register(binder, Server.class);
          }

          private void configureTaskStorage(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.storage", TaskStorageConfig.class);

            PolyBind.createChoice(
                binder, "druid.indexer.storage.type", Key.get(TaskStorage.class), Key.get(HeapMemoryTaskStorage.class)
            );
            final MapBinder<String, TaskStorage> storageBinder = PolyBind.optionBinder(
                binder,
                Key.get(TaskStorage.class)
            );

            storageBinder.addBinding("local").to(HeapMemoryTaskStorage.class);
            binder.bind(HeapMemoryTaskStorage.class).in(LazySingleton.class);

            storageBinder.addBinding("metadata").to(MetadataTaskStorage.class).in(ManageLifecycle.class);
            binder.bind(MetadataTaskStorage.class).in(LazySingleton.class);
          }

          private void configureRunners(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.worker", WorkerConfig.class);

            PolyBind.createChoice(
                binder,
                "druid.indexer.runner.type",
                Key.get(TaskRunnerFactory.class),
                Key.get(ForkingTaskRunnerFactory.class)
            );
            final MapBinder<String, TaskRunnerFactory> biddy = PolyBind.optionBinder(
                binder,
                Key.get(TaskRunnerFactory.class)
            );

            IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);
            biddy.addBinding("local").to(ForkingTaskRunnerFactory.class);
            binder.bind(ForkingTaskRunnerFactory.class).in(LazySingleton.class);

            biddy.addBinding(RemoteTaskRunnerFactory.TYPE_NAME)
                 .to(RemoteTaskRunnerFactory.class)
                 .in(LazySingleton.class);
            binder.bind(RemoteTaskRunnerFactory.class).in(LazySingleton.class);

            JacksonConfigProvider.bind(binder, WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class, null);
          }

          private void configureAutoscale(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.autoscale", ResourceManagementSchedulerConfig.class);
            JsonConfigProvider.bind(
                binder,
                "druid.indexer.autoscale",
                PendingTaskBasedWorkerResourceManagementConfig.class
            );
            JsonConfigProvider.bind(binder, "druid.indexer.autoscale", SimpleWorkerResourceManagementConfig.class);

            PolyBind.createChoice(
                binder,
                "druid.indexer.autoscale.strategy.type",
                Key.get(ResourceManagementStrategy.class),
                Key.get(SimpleWorkerResourceManagementStrategy.class)
            );
            final MapBinder<String, ResourceManagementStrategy> biddy = PolyBind.optionBinder(
                binder,
                Key.get(ResourceManagementStrategy.class)
            );
            biddy.addBinding("simple").to(SimpleWorkerResourceManagementStrategy.class);
            biddy.addBinding("pendingTaskBased").to(PendingTaskBasedWorkerResourceManagementStrategy.class);

          }

          private void configureOverlordHelpers(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.logs.kill", TaskLogAutoCleanerConfig.class);
            Multibinder.newSetBinder(binder, OverlordHelper.class)
                       .addBinding()
                       .to(TaskLogAutoCleaner.class);
          }
        },
        new IndexingServiceFirehoseModule(),
        new IndexingServiceTaskLogsModule(),
        new AdminModule(this)
    );
  }

  /**
   */
  private static class OverlordJettyServerInitializer implements JettyServerInitializer
  {
    private final AuthConfig authConfig;
    private final ServerConfig serverConfig;

    @Inject
    OverlordJettyServerInitializer(AuthConfig authConfig, ServerConfig serverConfig)
    {
      this.authConfig = authConfig;
      this.serverConfig = serverConfig;
    }

    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
      root.setInitParameter("org.eclipse.jetty.servlet.Default.redirectWelcome", "true");
      root.setWelcomeFiles(new String[]{"console.html"});
      root.addEventListener(new GuiceServletConfig(injector));

      ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);

      root.addServlet(holderPwd, "/");
      root.setBaseResource(
          new ResourceCollection(
              Resource.newClassPathResource("org/apache/druid/console")
          )
      );

      final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
      final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

      AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

      // perform no-op authorization/authentication for these resources
      AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, UNSECURED_PATHS);
      AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, authConfig.getUnsecuredPaths());

      final List<Authenticator> authenticators = authenticatorMapper.getAuthenticatorChain();
      AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

      AuthenticationUtils.addAllowOptionsFilter(root, authConfig.isAllowUnauthenticatedHttpOptions());

      JettyServerInitUtils.addExtensionFilters(root, injector);


      // Check that requests were authorized before sending responses
      AuthenticationUtils.addPreResponseAuthorizationCheckFilter(root, authenticators, jsonMapper);

      // add some paths not to be redirected to leader.
      root.addFilter(DelegatedGuiceFilter.class, "/status/*", null);
      root.addFilter(DelegatedGuiceFilter.class, "/druid-internal/*", null);
      root.addFilter(DelegatedGuiceFilter.class, "/druid/admin/*", null);

      // redirect anything other than status to the current lead
      root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);

      // Can't use /* here because of Guice and Jetty static content conflicts
      root.addFilter(DelegatedGuiceFilter.class, "/druid/*", null);

      root.addFilter(DelegatedGuiceFilter.class, "/druid-ext/*", null);

      HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{
          JettyServerInitUtils.getJettyRequestLogHandler(),
          JettyServerInitUtils.wrapWithDefaultGzipHandler(root)
      });

      server.setHandler(handlerList);
    }
  }
}
