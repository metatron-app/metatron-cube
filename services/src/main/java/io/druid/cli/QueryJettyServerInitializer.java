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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.DelegatedGuiceFilter;
import io.druid.server.GuiceServletConfig;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.List;
import java.util.Set;

/**
 */
public class QueryJettyServerInitializer implements JettyServerInitializer
{
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
    root.addEventListener(new GuiceServletConfig(injector));
    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    JettyServerInitUtils.addExtensionFilters(root, injector);

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

    server.setHandler(handlerList);
  }
}
