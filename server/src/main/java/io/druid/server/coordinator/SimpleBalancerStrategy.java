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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.data.Pair;
import io.druid.timeline.DataSegment;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
    final int serverCount = serverHolders.size();
    final Set<String> dataSourceNames = Sets.newHashSet();
    final ImmutableDruidServer[] servers = new ImmutableDruidServer[serverCount];
    for (int i = 0; i < serverCount; i++) {
      servers[i] = serverHolders.get(i).getServer();
      Iterables.addAll(dataSourceNames, servers[i].getDataSourceNames());
    }
    final int maxSegmentsToMove = params.getCoordinatorDynamicConfig().getMaxSegmentsToMove();
    final List<Pair<BalancerSegmentHolder, ImmutableDruidServer>> found = Lists.newArrayList();

    for (String dataSourceName : dataSourceNames) {
      List<Pair<Integer, DataSegment>> allSegments = Lists.newArrayList();
      for (int i = 0; i < serverCount; i++) {
        ImmutableDruidDataSource dataSource = servers[i].getDataSource(dataSourceName);
        if (dataSource != null) {
          for (DataSegment segment : dataSource.getSegments()) {
            allSegments.add(Pair.of(i, segment));
          }
        }
      }
      Collections.sort(
          allSegments,
          Ordering.from(DruidCoordinator.SEGMENT_COMPARATOR).onResultOf(Pair.<DataSegment>rhs())
      );
      @SuppressWarnings("unchecked")
      final List<DataSegment>[] segmentsPerServer = (List<DataSegment>[]) Array.newInstance(List.class, serverCount);
      for (int i = 0; i < serverCount; i++) {
        segmentsPerServer[i] = Lists.newArrayList();
      }
      final int multiplier = 2;   // parameterize ?
      final int reservoirSize = serverCount * multiplier;
      final List<Integer> empty = Lists.newArrayList();
      final List<Integer> excessive = Lists.newArrayList();
      final Iterator<Pair<Integer, DataSegment>> iterator = allSegments.iterator();
      while (found.size() < maxSegmentsToMove && iterator.hasNext()) {
        for (int i = 0; i < reservoirSize && iterator.hasNext(); i++) {
          Pair<Integer, DataSegment> pair = iterator.next();
          segmentsPerServer[pair.lhs].add(pair.rhs);
        }
        for (int i = 0; i < serverCount; i++) {
          final int size = segmentsPerServer[i].size();
          if (size < multiplier) {
            empty.add(i);
          } else if (size > multiplier) {
            excessive.add(i);
          }
        }
        if (empty.isEmpty() || excessive.isEmpty()) {
          continue;
        }
        Iterator<Integer> emptyIterator = empty.iterator();
        int to = emptyIterator.next();
        for (int from : excessive) {
          while (found.size() < maxSegmentsToMove && segmentsPerServer[from].size() > multiplier) {
            if (segmentsPerServer[to].size() >= multiplier) {
              to = emptyIterator.next();
            }
            DataSegment segment = segmentsPerServer[from].remove(segmentsPerServer[from].size() - 1);
            LOG.debug(
                "Balancing segment[%s:%s] : from %s to %s",
                segment.getDataSource(),
                segment.getInterval(),
                servers[from].getName(),
                servers[to].getName()
            );
            found.add(Pair.of(new BalancerSegmentHolder(servers[from], segment), servers[to]));

            segmentsPerServer[to].add(segment);
          }
        }
        for (int i = 0; i < serverCount; i++) {
          segmentsPerServer[i].clear();
        }
        excessive.clear();
        empty.clear();
      }
    }
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
