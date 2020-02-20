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

package io.druid.client.indexing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.client.ServiceClient;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.EscalatedGlobal;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IndexingServiceClient extends ServiceClient
{
  @Inject
  public IndexingServiceClient(
      @EscalatedGlobal HttpClient client,
      ObjectMapper jsonMapper,
      @IndexingService ServerDiscoverySelector selector
  )
  {
    super("/druid/indexer/v1", client, jsonMapper, selector);
  }

  public Pair<String, URL> mergeSegments(List<DataSegment> segments)
  {
    final Iterator<DataSegment> segmentsIter = segments.iterator();
    if (!segmentsIter.hasNext()) {
      return null;
    }

    final String dataSource = segmentsIter.next().getDataSource();
    while (segmentsIter.hasNext()) {
      DataSegment next = segmentsIter.next();
      if (!dataSource.equals(next.getDataSource())) {
        throw new IAE("Cannot merge segments of different dataSources[%s] and [%s]", dataSource, next.getDataSource());
      }
    }

    return runQuery(new ClientAppendQuery(dataSource, segments));
  }

  public void killSegments(String dataSource, Interval interval)
  {
    runQuery(new ClientKillQuery(dataSource, interval));
  }

  public void upgradeSegment(DataSegment dataSegment)
  {
    runQuery(new ClientConversionQuery(dataSegment));
  }

  public void upgradeSegments(String dataSource, Interval interval)
  {
    runQuery(new ClientConversionQuery(dataSource, interval));
  }

  private Pair<String, URL> runQuery(Object queryObject)
  {
    Request request = makeRequest(HttpMethod.POST, "task");
    Map<String, String> result = execute(request, queryObject, new TypeReference<Map<String, String>>() {});
    if (result != null) {
      return Pair.of(result.get("task"), request.getUrl());
    }
    return null;
  }

  public boolean isFinished(String taskId, URL baseUrl)
  {
    try {
      URL url = new URL(baseUrl + "/" + taskId);
      Request request = new Request(HttpMethod.POST, url);
      StatusResponseHolder response = client.go(request, RESPONSE_HANDLER).get();
      return response.getStatus().equals(HttpResponseStatus.NOT_FOUND);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
