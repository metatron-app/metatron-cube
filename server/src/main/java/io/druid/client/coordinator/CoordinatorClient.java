/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
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

package io.druid.client.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.ImmutableSegmentLoadInfo;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Self;
import io.druid.server.DruidNode;
import io.druid.timeline.DataSegment;
import net.spy.memcached.util.StringUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CoordinatorClient
{
  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);

  private final DruidNode server;
  private final HttpClient client;
  private final ObjectMapper jsonMapper;
  private final ServerDiscoverySelector selector;

  @Inject
  public CoordinatorClient(
      @Self DruidNode server,
      @Global HttpClient client,
      ObjectMapper jsonMapper,
      @Coordinator ServerDiscoverySelector selector
  )
  {
    this.server = server;
    this.client = client;
    this.jsonMapper = jsonMapper;
    this.selector = selector;
  }

  public List<String> findDatasources(List<String> dataSources)
  {
    return execute(
        HttpMethod.GET,
        String.format("/datasources/?nameRegex=%s", StringUtils.join(dataSources, ",")),
        new TypeReference<List<String>>()
        {
        }
    );
  }

  public List<ImmutableSegmentLoadInfo> fetchServerView(String dataSource, Interval interval, boolean incompleteOk)
  {
    return execute(
        HttpMethod.GET,
        String.format("/datasources/%s/intervals/%s/serverview?partial=%s",
                      dataSource,
                      interval.toString().replace("/", "_"),
                      incompleteOk),
        new TypeReference<List<ImmutableSegmentLoadInfo>>()
        {
        }
    );
  }

  public <T> T fetchTableDesc(String dataSource, String extractType, TypeReference<T> resultType)
  {
    return execute(
        HttpMethod.GET,
        String.format("/datasources/%s/desc/%s", dataSource, extractType),
        resultType
    );
  }

  public Map<String, Object> scheduleNow(Set<DataSegment> segments, long waitTimeout, boolean assertLoaded)
  {
    String resource = String.format("/scheduleNow?assertLoaded=%s&waitTimeout=%s", assertLoaded, waitTimeout);
    return execute(HttpMethod.POST, resource, segments, new TypeReference<Map<String, Object>>() {});
  }

  public StatusResponseHolder reportFileNotFound(DataSegment[] segments)
  {
    String resource = String.format("/report/segment/FileNotFound/%s", server.getHostAndPort());
    try {
      return execute(HttpMethod.POST, new URL(baseUrl() + resource), segments);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private <T> T execute(HttpMethod method, String resource, TypeReference<T> resultType)
  {
    return execute(method, resource, null, resultType);
  }

  private <T> T execute(HttpMethod method, String resource, Object payload, TypeReference<T> resultType)
  {
    try {
     URL coordinatorURL = new URL(baseUrl() + resource);
     StatusResponseHolder response = execute(method, coordinatorURL, payload);
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while executing [%s].. status[%s] content[%s]",
            coordinatorURL,
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

  private StatusResponseHolder execute(HttpMethod method, URL coordinatorURL, Object payload)
  {
    try {
      Request request = new Request(method, coordinatorURL);
      if (payload != null) {
        request.setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(payload));
      }
      return client.go(request, RESPONSE_HANDLER).get();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String baseUrl()
  {
    try {
      final Server instance = selector.pick();
      if (instance == null) {
        throw new ISE("Cannot find instance of coordinator.. Did you set `druid.selectors.coordinator.serviceName`?");
      }

      return new URI(
          instance.getScheme(),
          null,
          instance.getAddress(),
          instance.getPort(),
          "/druid/coordinator/v1",
          null,
          null
      ).toString();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Make a Request object aimed at the leader. Throws IOException if the leader cannot be located.
   */
  public Request makeRequest(HttpMethod httpMethod, String urlPath) throws IOException
  {
    return new Request(httpMethod, new URL(io.druid.common.utils.StringUtils.format("%s%s", baseUrl(), urlPath)));
  }

  /**
   * Executes the request object aimed at the leader and process the response with given handler
   * Note: this method doesn't do retrying on errors or handle leader changes occurred during communication
   */
  public <Intermediate, Final> ListenableFuture<Final> goAsync(
      final Request request,
      final HttpResponseHandler<Intermediate, Final> handler
  )
  {
    return client.go(request, handler);
  }
}
