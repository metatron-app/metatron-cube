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
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.client.ImmutableSegmentLoadInfo;
import io.druid.client.ServiceClient;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.EscalatedGlobal;
import io.druid.guice.annotations.Self;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.server.DruidNode;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class CoordinatorClient extends ServiceClient
{
  private final DruidNode server;

  @Inject
  public CoordinatorClient(
      @Self DruidNode server,
      @EscalatedGlobal HttpClient client,
      ObjectMapper jsonMapper,
      @Coordinator ServerDiscoverySelector selector
  )
  {
    super("/druid/coordinator/v1", client, jsonMapper, selector);
    this.server = server;
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
      return execute(makeRequest(HttpMethod.POST, resource), segments);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
