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

package io.druid.server.coordinator;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.common.DateTimes;
import io.druid.data.Pair;
import io.druid.granularity.Granularities;
import io.druid.timeline.DataSegment;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
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
    if (serverHolders.isEmpty()) {
      return Arrays.asList();
    }
    for (ServerHolder holder : serverHolders) {
      if (holder.getPeon().isDoingSomething()) {
        return Arrays.asList();   // busy.. balance later
      }
    }
    final long start = Granularities.DAY.bucketStart(DateTimes.nowUtc()).getMillis();
    final int serverCount = serverHolders.size();
    final Set<String> dataSourceNames = Sets.newTreeSet();  // just to estimate progress
    final ImmutableDruidServer[] servers = new ImmutableDruidServer[serverCount];
    for (int i = 0; i < serverCount; i++) {
      servers[i] = serverHolders.get(i).getServer();
      Iterables.addAll(dataSourceNames, servers[i].getDataSourceNames());
    }
    final int maxSegmentsToMove = params.getMaxSegmentsToMove();
    final List<Pair<BalancerSegmentHolder, ImmutableDruidServer>> found = Lists.newArrayList();

    for (String dataSourceName : dataSourceNames) {
      List<Pair<Integer, DataSegment>> allSegments = Lists.newArrayList();
      for (int i = 0; i < serverCount; i++) {
        ImmutableDruidDataSource dataSource = servers[i].getDataSource(dataSourceName);
        if (dataSource != null) {
          for (DataSegment segment : dataSource.getSegments()) {
            if (segment.getInterval().getEndMillis() <= start) {
              allSegments.add(Pair.of(i, segment));
            }
          }
        }
      }
      Collections.sort(
          allSegments,
          Ordering.from(DruidCoordinator.SEGMENT_COMPARATOR).onResultOf(Pair.<DataSegment>rhs())
      );
      @SuppressWarnings("unchecked")
      final List<DataSegment>[] segmentsPerServer = (List<DataSegment>[]) Array.newInstance(List.class, serverCount);
      for (int x = 0; x < serverCount; x++) {
        segmentsPerServer[x] = Lists.newArrayList();
      }

      final int numSegments = allSegments.size();
      final int groupPerServer = Math.max(1, numSegments / serverCount / 12);
      final int tolerance = Math.max(1, groupPerServer / 6);

      final List<Integer> deficit = Lists.newArrayList();
      final List<Integer> excessive = Lists.newArrayList();

      final int[] countsPerServer = new int[serverCount];
      int i = 0;
      for (int group = 1; found.size() < maxSegmentsToMove && i < numSegments; group++) {
        final int expected = group * groupPerServer;
        if (group > 1) {
          for (int x = 0; x < serverCount; x++) {
            segmentsPerServer[x].clear();
          }
          excessive.clear();
          deficit.clear();
        }

        int limit = Math.min(numSegments, i + groupPerServer * serverCount);
        for (; i < limit; i++) {
          Pair<Integer, DataSegment> pair = allSegments.get(i);
          segmentsPerServer[pair.lhs].add(pair.rhs);
          countsPerServer[pair.lhs]++;
        }
        for (int x = 0; x < serverCount; x++) {
          int count = countsPerServer[x];
          for (; count < expected; count++) {
            deficit.add(x);
          }
          for (; count > expected + tolerance; count--) {
            excessive.add(x);
          }
        }
        if (excessive.isEmpty()) {
          continue;
        }

        Collections.shuffle(deficit);
        Collections.shuffle(excessive);

        for (int from : excessive) {
          for (int to : deficit) {
            if (servers[to].isDecommissioned() || segmentsPerServer[to].size() >= groupPerServer) {
              continue;
            }
            DataSegment segment = findTarget(servers[to], segmentsPerServer[from], segmentsPerServer[to]);
            if (segment == null) {
              continue;
            }
            LOG.debug(
                "Balancing segment[%s:%s] : from %s to %s",
                segment.getDataSource(),
                segment.getInterval(),
                servers[from].getName(),
                servers[to].getName()
            );
            found.add(Pair.of(new BalancerSegmentHolder(servers[from], segment), servers[to]));

            countsPerServer[from]--;
            countsPerServer[to]++;
            break;
          }
        }
      }
    }
    return found;
  }

  private DataSegment findTarget(ImmutableDruidServer server, List<DataSegment> from, List<DataSegment> to)
  {
    for (int i = from.size() - 1; i >= 0; i--) {
      DataSegment segment = from.get(i);
      if (!to.contains(segment) && !server.contains(segment)) {
        from.remove(i);
        to.add(segment);
        return segment;
      }
    }
    return null;
  }

  @Override
  public ServerHolder findNewSegmentHomeReplicator(
      DataSegment proposalSegment, List<ServerHolder> serverHolders
  )
  {
    // can be used for bulk loading when server is down.. need to handle that properly
    int minExpectedSegments = -1;
    ServerHolder minServer = null;
    for (ServerHolder holder : RandomBalancerStrategy.filter(proposalSegment, serverHolders, false)) {
      int expectedSegments = holder.getNumExpectedSegments();
      if (minExpectedSegments < 0 || minExpectedSegments > expectedSegments) {
        minExpectedSegments = expectedSegments;
        minServer = holder;
      }
    }
    return minServer;
  }
}
