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
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.client.ServiceClient;
import io.druid.common.utils.StringUtils;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.EscalatedGlobal;
import io.druid.indexer.TaskStatusPlus;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.FullResponseHandler;
import io.druid.java.util.http.client.response.FullResponseHolder;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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

  public List<TaskStatusPlus> getRunningTasks()
  {
    return getTasks("runningTasks");
  }

  public List<TaskStatusPlus> getPendingTasks()
  {
    return getTasks("pendingTasks");
  }

  public List<TaskStatusPlus> getWaitingTasks()
  {
    return getTasks("waitingTasks");
  }

  private List<TaskStatusPlus> getTasks(String endpointSuffix)
  {
    try {
      final FullResponseHolder responseHolder = client.go(
              makeRequest(HttpMethod.GET, StringUtils.format("/%s", endpointSuffix)),
              new FullResponseHandler(Charsets.UTF_8)
      ).get();

      if (!responseHolder.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Error while fetching the status of the last complete task");
      }

      return jsonMapper.readValue(
              responseHolder.getContent(),
              new TypeReference<List<TaskStatusPlus>>()
              {
              }
      );
    }
    catch (IOException | InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  public TaskStatusPlus getLastCompleteTask()
  {
    final List<TaskStatusPlus> completeTaskStatuses = getTasks("completeTasks?n=1");
    return completeTaskStatuses.isEmpty() ? null : completeTaskStatuses.get(0);
  }

  public int killPendingSegments(String dataSource, DateTime end)
  {
    final String endPoint = StringUtils.format(
            "/pendingSegments/%s?interval=%s",
            dataSource,
            new Interval(DateTimes.MIN, end)
    );
    try {
      final FullResponseHolder responseHolder = client.go(
              makeRequest(HttpMethod.DELETE, endPoint),
              new FullResponseHandler(Charsets.UTF_8)
      ).get();

      if (!responseHolder.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Error while killing pendingSegments of dataSource[%s] created until [%s]", dataSource, end);
      }

      final Map<String, Object> resultMap = jsonMapper.readValue(
              responseHolder.getContent(),
              new TypeReference<Map<String, Object>>() { }
      );

      final Object numDeletedObject = resultMap.get("numDeleted");
      return (Integer) Preconditions.checkNotNull(numDeletedObject, "numDeletedObject");
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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
