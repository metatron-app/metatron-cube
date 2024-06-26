/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.servlet.DelegatedGuiceFilter;
import io.druid.guice.annotations.Json;
import io.druid.server.GuiceServletConfig;
import io.druid.server.coordinator.DruidCoordinatorConfig;
import io.druid.server.http.OverlordProxyServlet;
import io.druid.server.http.RedirectFilter;
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

import java.util.List;

/**
 */
class CoordinatorJettyServerInitializer implements JettyServerInitializer
{
  private static List<String> UNSECURED_PATHS = ImmutableList.of(
      "/coordinator/false",
      "/overlord/false",
      "/status/health",
      "/druid/coordinator/v1/isLeader"
  );

  private final DruidCoordinatorConfig config;

  @Inject
  CoordinatorJettyServerInitializer(DruidCoordinatorConfig config)
  {
    this.config = config;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    root.setInitParameter("org.eclipse.jetty.servlet.Default.redirectWelcome", "true");
    // index.html is the welcome file for old-console
    root.setWelcomeFiles(new String[]{"index.html"});
    root.addEventListener(new GuiceServletConfig(injector));

    ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);

    root.addServlet(holderPwd, "/");
    if(config.getConsoleStatic() == null) {
      root.setBaseResource(Resource.newClassPathResource("org/apache/druid/console"));
    } else {
      // used for console development
      root.setResourceBase(config.getConsoleStatic());
    }

    final AuthConfig authConfig = injector.getInstance(AuthConfig.class);
    final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

    // perform no-op authorization/authentication for these resources
    AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, UNSECURED_PATHS);
    AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, authConfig.getUnsecuredPaths());

    List<Authenticator> authenticators = authenticatorMapper.getAuthenticatorChain();
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

    // The coordinator really needs a standarized api path
    // Can't use '/*' here because of Guice and Jetty static content conflicts
    root.addFilter(DelegatedGuiceFilter.class, "/info/*", null);
    root.addFilter(DelegatedGuiceFilter.class, "/druid/coordinator/*", null);
    root.addFilter(DelegatedGuiceFilter.class, "/druid-ext/*", null);

    // this will be removed in the next major release
    root.addFilter(DelegatedGuiceFilter.class, "/coordinator/*", null);

    root.addServlet(new ServletHolder(injector.getInstance(OverlordProxyServlet.class)), "/druid/indexer/*");

    HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{
        JettyServerInitUtils.getJettyRequestLogHandler(),
        JettyServerInitUtils.wrapWithDefaultGzipHandler(root)
    });

    server.setHandler(handlerList);
  }
}
