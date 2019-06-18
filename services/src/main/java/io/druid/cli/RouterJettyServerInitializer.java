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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.DelegatedGuiceFilter;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.server.AsyncManagementForwardingServlet;
import io.druid.server.AsyncQueryForwardingServlet;
import io.druid.server.GuiceServletConfig;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.log.RequestLogger;
import io.druid.server.router.ManagementProxyConfig;
import io.druid.server.router.QueryHostFinder;
import io.druid.server.router.Router;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

import javax.servlet.Servlet;

/**
 */
public class RouterJettyServerInitializer implements JettyServerInitializer
{
  private static final EmittingLogger log = new EmittingLogger(AsyncQueryForwardingServlet.class);
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QueryHostFinder hostFinder;
  private final DruidHttpClientConfig routerHttpClientConfig;
  private final DruidHttpClientConfig globalHttpClientConfig;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final ManagementProxyConfig managementProxyConfig;
  private final AsyncQueryForwardingServlet asyncQueryForwardingServlet;
  private final AsyncManagementForwardingServlet asyncManagementForwardingServlet;


  @Inject
  public RouterJettyServerInitializer(
      @Router DruidHttpClientConfig routerHttpClientConfig,
      @Global DruidHttpClientConfig globalHttpClientConfig,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      ManagementProxyConfig managementProxyConfig,
      AsyncQueryForwardingServlet asyncQueryForwardingServlet,
      AsyncManagementForwardingServlet asyncManagementForwardingServlet
  )
  {
    this.routerHttpClientConfig = routerHttpClientConfig;
    this.globalHttpClientConfig = globalHttpClientConfig;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.managementProxyConfig = managementProxyConfig;
    this.asyncQueryForwardingServlet = asyncQueryForwardingServlet;
    this.asyncManagementForwardingServlet = asyncManagementForwardingServlet;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    root.setInitParameter("org.eclipse.jetty.servlet.Default.redirectWelcome", "true");
    // index.html is the welcome file for old-console
    root.setWelcomeFiles(new String[]{"unified-console.html", "index.html"});

    root.addEventListener(new GuiceServletConfig(injector));

    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    asyncQueryForwardingServlet.setTimeout(routerHttpClientConfig.getReadTimeout().getMillis());
    root.addServlet(buildServletHolder(asyncQueryForwardingServlet, routerHttpClientConfig), "/druid/v2/*");

    if (managementProxyConfig.isEnabled()) {
      ServletHolder managementForwardingServletHolder = buildServletHolder(
          asyncManagementForwardingServlet,
          globalHttpClientConfig
      );
      root.addServlet(managementForwardingServletHolder, "/druid/coordinator/*");
      root.addServlet(managementForwardingServletHolder, "/druid/indexer/*");
      root.addServlet(managementForwardingServletHolder, "/proxy/*");
    }

    if (managementProxyConfig.isEnabled()) {
      root.setBaseResource(Resource.newClassPathResource("org/apache/druid/console"));
    }

    JettyServerInitUtils.addExtensionFilters(root, injector);
    // Can't use '/*' here because of Guice conflicts with AsyncQueryForwardingServlet path
    root.addFilter(DelegatedGuiceFilter.class, "/status/*", null);
    root.addFilter(GuiceFilter.class, "/druid/router/*", null);
    root.addFilter(GuiceFilter.class, "/druid-ext/*", null);

    final HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{
        JettyServerInitUtils.getJettyRequestLogHandler(),
        JettyServerInitUtils.wrapWithDefaultGzipHandler(root)
    });
    server.setHandler(handlerList);
  }

  private ServletHolder buildServletHolder(Servlet servlet, DruidHttpClientConfig httpClientConfig)
  {
    ServletHolder sh = new ServletHolder(servlet);

    //NOTE: explicit maxThreads to workaround https://tickets.puppetlabs.com/browse/TK-152
    sh.setInitParameter("maxThreads", Integer.toString(httpClientConfig.getNumMaxThreads()));

    //Needs to be set in servlet config or else overridden to default value in AbstractProxyServlet.createHttpClient()
    sh.setInitParameter("maxConnections", Integer.toString(httpClientConfig.getNumConnections()));
    sh.setInitParameter("idleTimeout", Long.toString(httpClientConfig.getReadTimeout().getMillis()));
    sh.setInitParameter("timeout", Long.toString(httpClientConfig.getReadTimeout().getMillis()));

    return sh;
  }
}
