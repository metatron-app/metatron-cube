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
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.servlet.DelegatedGuiceFilter;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.GuiceServletConfig;
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
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;

import javax.servlet.DispatcherType;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class QueryJettyServerInitializer implements JettyServerInitializer
{
  private static final Logger log = new Logger(QueryJettyServerInitializer.class);
  private static List<String> UNSECURED_PATHS = Lists.newArrayList(
      "/status/health",
      "/druid/historical/v1/readiness",
      "/druid/broker/v1/readiness"
  );

  private final List<Handler> extensionHandlers;

  @Inject
  public QueryJettyServerInitializer(Set<Handler> extensionHandlers)
  {
    this.extensionHandlers = ImmutableList.copyOf(extensionHandlers);
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);

    for (String path : injector.getInstance(ServerConfig.class).getAllowCorsPaths()) {
      FilterHolder holder = root.addFilter(CrossOriginFilter.class, path, EnumSet.allOf(DispatcherType.class));
      holder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
      holder.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin");
      holder.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "GET,PUT,POST,DELETE,OPTIONS");
      holder.setInitParameter(CrossOriginFilter.PREFLIGHT_MAX_AGE_PARAM, "5184000");
      holder.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM, "true");
    }

    root.addEventListener(new GuiceServletConfig(injector));
    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    final AuthConfig authConfig = injector.getInstance(AuthConfig.class);
    final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

    // perform no-op authorization for these resources
    AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, UNSECURED_PATHS);
    AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, authConfig.getUnsecuredPaths());

    List<Authenticator> authenticators = authenticatorMapper.getAuthenticatorChain();
    AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

    AuthenticationUtils.addAllowOptionsFilter(root, authConfig.isAllowUnauthenticatedHttpOptions());

    JettyServerInitUtils.addExtensionFilters(root, injector);

    // Check that requests were authorized before sending responses
    AuthenticationUtils.addPreResponseAuthorizationCheckFilter(root, authenticators, jsonMapper);

    root.addFilter(DelegatedGuiceFilter.class, "/*", null);

    final HandlerList handlerList = new HandlerList();

    // Do not change the order of the handlers that have already been added
    for (Handler handler : server.getHandlers()) {
      handlerList.addHandler(handler);
    }
    handlerList.addHandler(JettyServerInitUtils.getJettyRequestLogHandler());

    // Add all extension handlers
    for (Handler handler : extensionHandlers) {
      handlerList.addHandler(handler);
    }

    handlerList.addHandler(JettyServerInitUtils.wrapWithDefaultGzipHandler(root));

    final StatisticsHandler statisticsHandler = new StatisticsHandler();
    statisticsHandler.setHandler(handlerList);

    server.setHandler(statisticsHandler);
  }
}
