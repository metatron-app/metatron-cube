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

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.common.ISE;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.HttpResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.net.URL;

public class ServiceClient
{
  protected static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);

  protected final String basePath;
  protected final HttpClient client;
  protected final ObjectMapper jsonMapper;
  protected final ServerDiscoverySelector selector;

  public ServiceClient(
      String basePath,
      HttpClient client,
      ObjectMapper jsonMapper,
      ServerDiscoverySelector selector
  )
  {
    this.basePath = basePath;
    this.client = client;
    this.jsonMapper = jsonMapper;
    this.selector = selector;
  }

  public <T> T execute(HttpMethod method, String resource, TypeReference<T> resultType)
  {
    return execute(method, resource, null, resultType);
  }

  public <T> T execute(HttpMethod method, String resource, Object payload, TypeReference<T> resultType)
  {
    return execute(makeRequest(method, resource), payload, resultType);
  }

  public <T> T execute(Request request, Object payload, TypeReference<T> resultType)
  {
    StatusResponseHolder response = execute(request, payload);
    try {
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while executing [%s].. status[%s] content[%s]",
            request.getUrl(),
            response.getStatus(),
            response.getContent()
        );
      }
      return jsonMapper.readValue(response.getContent(), resultType);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public StatusResponseHolder execute(Request request, Object payload)
  {
    try {
      if (payload != null) {
        request.setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(payload));
      }
      return client.go(request, RESPONSE_HANDLER).get();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Executes the request object aimed at the leader and process the response with given handler
   * Note: this method doesn't do retrying on errors or handle leader changes occurred during communication
   */
  public <Intermediate, Final> ListenableFuture<Final> goAsync(
      Request request, HttpResponseHandler<Intermediate, Final> handler
  )
  {
    return client.go(request, handler);
  }

  /**
   * Make a Request object aimed at the leader. Throws IOException if the leader cannot be located.
   */
  public Request makeRequest(HttpMethod method, String resourcePath)
  {
    try {
      Server instance = selector.pick();
      if (instance == null) {
        throw new ISE("Cannot find instance of coordinator.. Did you set `druid.selectors.coordinator.serviceName`?");
      }

      URI baseURI = new URI(
          instance.getScheme(), null, instance.getAddress(), instance.getPort(), basePath, null, null
      );
      if (!resourcePath.startsWith("/")) {
        resourcePath = "/" + resourcePath;
      }
      return new Request(method, new URL(baseURI + resourcePath));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
