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
import com.google.inject.name.Names;
import com.google.inject.servlet.DelegatedGuiceFilter;
import io.airlift.airline.Command;
import io.druid.audit.AuditManager;
import io.druid.guice.IndexingServiceFirehoseModule;
import io.druid.guice.IndexingServiceTaskLogsModule;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.indexing.overlord.http.OverlordRedirectInfo;
import io.druid.java.util.common.logger.Logger;
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

            binder.bind(AuditManager.class)
                  .toProvider(AuditManagerProvider.class)
                  .in(ManageLifecycle.class);

            binder.bind(RedirectFilter.class).in(LazySingleton.class);
            binder.bind(RedirectInfo.class).to(OverlordRedirectInfo.class).in(LazySingleton.class);
            binder.bind(JettyServerInitializer.class)
                  .to(OverlordJettyServerInitializer.class)
                  .in(LazySingleton.class);

            LifecycleModule.register(binder, Server.class);
          }
        },
        new OverlordModule(),
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
