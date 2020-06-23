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

package io.druid.server.coordinator.cost;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ClusterCostCache
{
  private final Map<String, ServerCostCache> serversCostCache;

  ClusterCostCache(Map<String, ServerCostCache> serversCostCache)
  {
    this.serversCostCache = Preconditions.checkNotNull(serversCostCache);
  }

  public double computeCost(String serverName, DataSegment dataSegment)
  {
    ServerCostCache serverCostCache = serversCostCache.get(serverName);
    return (serverCostCache != null) ? serverCostCache.computeCost(dataSegment) : 0.0;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(Map<String, LoadQueuePeon> segmentsByServerName)
  {
    final Builder builder = builder();
    segmentsByServerName.forEach(
        new BiConsumer<String, LoadQueuePeon>()
        {
          @Override
          public void accept(final String serverName, LoadQueuePeon peon)
          {
            peon.getSegmentsToLoad(segment -> builder.addSegment(serverName, segment));
          }
        }
    );
    return builder;
  }

  public static class Builder
  {
    private final Map<String, ServerCostCache.Builder> serversCostCache = new HashMap<>();

    public void addSegment(String serverName, DataSegment dataSegment)
    {
      ServerCostCache.Builder builder = serversCostCache.computeIfAbsent(
          serverName,
          new Function<String, ServerCostCache.Builder>()
          {
            @Override
            public ServerCostCache.Builder apply(String s)
            {
              return ServerCostCache.builder();
            }
          }
      );
      builder.addSegment(dataSegment);
    }

    public void removeSegment(String serverName, final DataSegment dataSegment)
    {
      serversCostCache.computeIfPresent(
          serverName,
          new BiFunction<String, ServerCostCache.Builder, ServerCostCache.Builder>()
          {
            @Override
            public ServerCostCache.Builder apply(String s, ServerCostCache.Builder builder)
            {
              return builder.removeSegment(dataSegment).isEmpty() ? null : builder;
            }
          }
      );
    }

    public void removeServer(String serverName)
    {
      serversCostCache.remove(serverName);
    }

    public ClusterCostCache build()
    {
      Map<String, ServerCostCache> costMap = Maps.newHashMap();
      for (Map.Entry<String, ServerCostCache.Builder> entry : serversCostCache.entrySet()) {
        costMap.put(entry.getKey(), entry.getValue().build());
      }
      return new ClusterCostCache(costMap);
    }
  }
}
