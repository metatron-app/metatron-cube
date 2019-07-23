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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.Request;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.common.utils.StringUtils;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.guice.http.DruidHttpClientConfig;
import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.proxy.AsyncProxyServlet;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URISyntaxException;

public class AsyncManagementForwardingServlet extends AsyncProxyServlet
{
  private static final EmittingLogger log = new EmittingLogger(AsyncManagementForwardingServlet.class);

  private static final String BASE_URI_ATTRIBUTE = "io.druid.proxy.to.base.uri";
  private static final String MODIFIED_PATH_ATTRIBUTE = "io.druid.proxy.to.path";

  // These are the typical path conventions for the coordinator and overlord APIs. If we see one of these paths, we will
  // forward the request with the path unmodified, e.g.:
  //   Client Request: https://{ROUTER_HOST}:9088/druid/coordinator/v1/loadstatus?full
  //   Proxy Request:  https://{COORDINATOR_HOST}:8281/druid/coordinator/v1/loadstatus?full
  private static final String STANDARD_COORDINATOR_BASE_PATH = "/druid/coordinator";
  private static final String STANDARD_OVERLORD_BASE_PATH = "/druid/indexer";

  // But there are some cases where the path is either ambiguous or collides with other servlet pathSpecs and where it
  // is desirable to explicitly state the destination host. In these cases, we will forward the request with the proxy
  // destination component of the path stripped, e.g.:
  //   Client Request: https://{ROUTER_HOST}:9088/proxy/coordinator/druid-ext/basic-security/authorization/db/b/users
  //   Proxy Request:  https://{COORDINATOR_HOST}:8281/druid-ext/basic-security/authorization/db/b/users
  private static final String ARBITRARY_COORDINATOR_BASE_PATH = "/proxy/coordinator";
  private static final String ARBITRARY_OVERLORD_BASE_PATH = "/proxy/overlord";

  private final ObjectMapper jsonMapper;
  private final Provider<HttpClient> httpClientProvider;
  private final DruidHttpClientConfig httpClientConfig;
  private final CoordinatorClient coordLeaderSelector;
  private final IndexingServiceClient overlordLeaderSelector;

  @Inject
  public AsyncManagementForwardingServlet(
      @Json ObjectMapper jsonMapper,
      @Global Provider<HttpClient> httpClientProvider,
      @Global DruidHttpClientConfig httpClientConfig,
      CoordinatorClient coordLeaderSelector,
      IndexingServiceClient overlordLeaderSelector
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClientProvider = httpClientProvider;
    this.httpClientConfig = httpClientConfig;
    this.coordLeaderSelector = coordLeaderSelector;
    this.overlordLeaderSelector = overlordLeaderSelector;
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
  {
    Request currentLeaderRequest;
    String requestURI = StringUtils.toLowerCase(request.getRequestURI());
    if (requestURI.startsWith(STANDARD_COORDINATOR_BASE_PATH)) {
      currentLeaderRequest = coordLeaderSelector.makeRequest(HttpMethod.GET, "/");
    } else if (requestURI.startsWith(STANDARD_OVERLORD_BASE_PATH)) {
      currentLeaderRequest = overlordLeaderSelector.makeRequest(HttpMethod.GET, "");
    } else if (requestURI.startsWith(ARBITRARY_COORDINATOR_BASE_PATH)) {
      currentLeaderRequest = coordLeaderSelector.makeRequest(HttpMethod.GET, "/");
      request.setAttribute(
          MODIFIED_PATH_ATTRIBUTE, request.getRequestURI().substring(ARBITRARY_COORDINATOR_BASE_PATH.length())
      );
    } else if (requestURI.startsWith(ARBITRARY_OVERLORD_BASE_PATH)) {
      currentLeaderRequest = overlordLeaderSelector.makeRequest(HttpMethod.GET, "/");
      request.setAttribute(
          MODIFIED_PATH_ATTRIBUTE, request.getRequestURI().substring(ARBITRARY_OVERLORD_BASE_PATH.length())
      );
    } else {
      handleBadRequest(response, StringUtils.format("Unsupported proxy destination [%s]", request.getRequestURI()));
      return;
    }

    if (currentLeaderRequest == null) {
      handleBadRequest(
          response,
          StringUtils.format(
              "Unable to determine destination for [%s]; is your coordinator/overlord running?", request.getRequestURI()
          )
      );
      return;
    }

    log.debug("currentLeaderRequest=%s", currentLeaderRequest.getUrl());
    request.setAttribute(BASE_URI_ATTRIBUTE, currentLeaderRequest.getUrl().toString());
    super.service(request, response);
  }

  @Override
  protected String rewriteTarget(HttpServletRequest request)
  {
    try {
      return new URIBuilder((String) request.getAttribute(BASE_URI_ATTRIBUTE))
          .setPath(request.getAttribute(MODIFIED_PATH_ATTRIBUTE) != null ?
                   (String) request.getAttribute(MODIFIED_PATH_ATTRIBUTE) : request.getRequestURI())
          .setQuery(request.getQueryString()) // No need to encode-decode queryString, it is already encoded
          .build()
          .toString();
    }
    catch (URISyntaxException e) {
      log.error(e, "Unable to rewrite URI [%s]", e.getMessage());
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected HttpClient newHttpClient()
  {
    return httpClientProvider.get();
  }

  @Override
  protected HttpClient createHttpClient() throws ServletException
  {
    HttpClient client = super.createHttpClient();
    setTimeout(httpClientConfig.getReadTimeout().getMillis()); // override timeout set in ProxyServlet.createHttpClient
    return client;
  }

  private void handleBadRequest(HttpServletResponse response, String errorMessage) throws IOException
  {
    if (!response.isCommitted()) {
      response.resetBuffer();
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      jsonMapper.writeValue(response.getOutputStream(), ImmutableMap.of("error", errorMessage));
    }
    response.flushBuffer();
  }
}
