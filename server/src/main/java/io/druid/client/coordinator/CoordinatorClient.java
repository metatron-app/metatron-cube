/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.client.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.ImmutableSegmentLoadInfo;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.Global;
import net.spy.memcached.util.StringUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import java.net.URI;
import java.net.URL;
import java.util.List;

public class CoordinatorClient
{
  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);

  private final HttpClient client;
  private final ObjectMapper jsonMapper;
  private final ServerDiscoverySelector selector;

  @Inject
  public CoordinatorClient(
      @Global HttpClient client,
      ObjectMapper jsonMapper,
      @Coordinator ServerDiscoverySelector selector
  )
  {
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

  private <T> T execute(HttpMethod method, String resource, TypeReference<T> resultType)
  {
    try {
      Request request = new Request(method, new URL(baseUrl() + resource));
      StatusResponseHolder response = client.go(request, RESPONSE_HANDLER).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching serverView status[%s] content[%s]",
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
}
