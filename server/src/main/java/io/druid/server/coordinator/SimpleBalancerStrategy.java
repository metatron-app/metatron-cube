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

package io.druid.server.coordinator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.data.Pair;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 */
public class SimpleBalancerStrategy extends BalancerStrategy.Abstract
{
  private static final Logger LOG = new Logger(SimpleBalancerStrategy.class);

  @Override
  public List<Pair<BalancerSegmentHolder, ImmutableDruidServer>> select(
      DruidCoordinatorRuntimeParams params,
      List<ServerHolder> serverHolders
  )
  {
    // ds - server - # of segments
    Map<String, Map<String, MutableInt>> dataSources = Maps.newHashMap();
    Map<String, ServerHolder> servers = Maps.newHashMap();
    for (ServerHolder holder : serverHolders) {
      ImmutableDruidServer server = holder.getServer();
      servers.put(server.getName(), holder);
      for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
        dataSources.computeIfAbsent(
            dataSource.getName(), new Function<String, Map<String, MutableInt>>()
            {
              @Override
              public Map<String, MutableInt> apply(String s)
              {
                return Maps.newHashMap();
              }
            }
        ).computeIfAbsent(
            server.getName(), new Function<String, MutableInt>()
            {
              @Override
              public MutableInt apply(String s)
              {
                return new MutableInt();
              }
            }
        ).add(dataSource.getSegments().size());
      }
    }
    // making servers have even numbers of segments for datasource
    final int maxSegmentsToMove = params.getCoordinatorDynamicConfig().getMaxSegmentsToMove();
    final List<Pair<BalancerSegmentHolder, ImmutableDruidServer>> found = Lists.newArrayList();
    for (Map.Entry<String, Map<String, MutableInt>> entry : dataSources.entrySet()) {
      int total = 0;
      MutableInt max = null;
      MutableInt min = null;
      String maxServer = null;
      String minServer = null;
      final String dataSource = entry.getKey();
      final Map<String, MutableInt> serverToCount = entry.getValue();
      for (Map.Entry<String, MutableInt> counters : serverToCount.entrySet()) {
        MutableInt count = counters.getValue();
        if (max == null || count.intValue() > max.intValue()) {
          max = count;
          maxServer = counters.getKey();
        }
        if (min == null || count.intValue() < min.intValue()) {
          min = count;
          minServer = counters.getKey();
        }
        total += count.intValue();
      }
      if (min == null || max == null) {
        continue;
      }
      final int mean = total / serverToCount.size() + 1;
      if (Math.min(max.intValue() - 1, mean) >= min.intValue() + 1) {
        LOG.debug(
            "Balancing dataSource[%s] : from %s (%d segments) to %s (%d segments)",
            dataSource, maxServer, max.intValue(), minServer, min.intValue());
      }
      // possible to recalculate min/max if needed
      while (found.size() < maxSegmentsToMove && Math.min(max.intValue() - 1, mean) >= min.intValue() + 1) {
        max.decrement();
        min.increment();
        ImmutableDruidServer from = servers.get(maxServer).getServer();
        ServerHolder to = servers.get(minServer);
        for (DataSegment segment : from.getDataSource(dataSource).getSegments()) {
          if (to.getAvailableSize() > segment.getSize()) {
            found.add(Pair.of(new BalancerSegmentHolder(from, segment), to.getServer()));
            break;
          }
        }
      }
    }
    // todo shuffle segments by interval (swap?)
    return found;
  }

  @Override
  public ServerHolder findNewSegmentHomeReplicator(
      DataSegment proposalSegment, List<ServerHolder> serverHolders
  )
  {
    // can be used for bulk loading when server is down.. need to handle that properly
    int min = -1;
    ServerHolder minServer = null;
    final String dataSourceName = proposalSegment.getDataSource();
    for (ServerHolder holder : RandomBalancerStrategy.filter(proposalSegment, serverHolders, false)) {
      int numSegments = 0;
      long availableSize = holder.getAvailableSize();
      for (DataSegment segment : holder.getPeon().getSegmentsToLoad()) {
        if (dataSourceName.equals(segment.getDataSource())) {
          availableSize -= segment.getSize();
          numSegments += 1;
        }
      }
      for (DataSegment segment : holder.getPeon().getSegmentsToDrop()) {
        if (dataSourceName.equals(segment.getDataSource())) {
          availableSize += segment.getSize();
          numSegments -= 1;
        }
      }
      if (availableSize < proposalSegment.getSize()) {
        continue;
      }
      ImmutableDruidDataSource dataSource = holder.getServer().getDataSource(dataSourceName);
      if (dataSource != null) {
        numSegments += dataSource.getSegments().size();
      }
      if (min < 0 || numSegments < min) {
        min = numSegments;
        minServer = holder;
      }
    }
    return minServer;
  }
}
