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
import io.druid.collections.IntList;
import io.druid.common.DateTimes;
import io.druid.data.Pair;
import io.druid.granularity.Granularities;
import io.druid.timeline.DataSegment;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * don't bother tier, capacity, etc. just simple balancing of segments.
 */
public class SimpleBalancerStrategy extends BalancerStrategy.Abstract
{
  private static final Logger LOG = new Logger(SimpleBalancerStrategy.class);

  private static final float BASELINE_RATIO = 0.85f;

  private static final int DS_GROUP_NUM = 8;
  private static final float DS_GROUP_TOLERANCE_RATIO = 0.15f;

  private final Random random = new Random();

  @Override
  public List<Pair<BalancerSegmentHolder, ImmutableDruidServer>> select(
      DruidCoordinatorRuntimeParams params,
      List<ServerHolder> serverHolders
  )
  {
    if (serverHolders.isEmpty()) {
      return Arrays.asList();
    }
    int numberOfQueuedSegments = 0;
    for (ServerHolder holder : serverHolders) {
      numberOfQueuedSegments += holder.getPeon().getNumberOfQueuedSegments();
    }
    final int maxSegmentsToMove = params.getMaxSegmentsToMove() - numberOfQueuedSegments;
    if (maxSegmentsToMove <= 0) {
      return Arrays.asList();
    }
    final long start = Granularities.DAY.bucketStart(DateTimes.nowUtc()).getMillis();
    final int serverCount = serverHolders.size();

    final Set<String> dataSourceNames = Sets.newTreeSet();  // sorted to estimate progress
    final ImmutableDruidServer[] servers = new ImmutableDruidServer[serverCount];
    final int[] totalSegmentsPerServer = new int[serverCount];
    int totalSegments = 0;
    for (int i = 0; i < serverCount; i++) {
      servers[i] = serverHolders.get(i).getServer();
      totalSegmentsPerServer[i] = servers[i].getSegments().size();
      totalSegments += totalSegmentsPerServer[i];
      Iterables.addAll(dataSourceNames, servers[i].getDataSourceNames());
    }
    final int baseLine = (int) (totalSegments / serverCount * BASELINE_RATIO);
    final List<Pair<BalancerSegmentHolder, ImmutableDruidServer>> found = Lists.newArrayList();

    // per DS, incremental
    final int[] totalSegmentsPerServerInDs = new int[serverCount];

    // per group
    final IntList deficit = new IntList();
    final IntList excessive = new IntList();

    @SuppressWarnings("unchecked")
    final List<DataSegment>[] segmentsPerServer = (List<DataSegment>[]) Array.newInstance(List.class, serverCount);
    for (int x = 0; x < serverCount; x++) {
      segmentsPerServer[x] = Lists.newArrayList();
    }

    for (String dataSourceName : dataSourceNames) {
      List<Pair<Integer, DataSegment>> allSegmentsInDS = Lists.newArrayList();
      for (int i = 0; i < serverCount; i++) {
        ImmutableDruidDataSource dataSource = servers[i].getDataSource(dataSourceName);
        if (dataSource != null) {
          for (DataSegment segment : dataSource.getSegments()) {
            if (segment.getInterval().getEndMillis() <= start) {
              allSegmentsInDS.add(Pair.of(i, segment));
            }
          }
        }
      }
      Collections.sort(
          allSegmentsInDS,
          Ordering.from(DruidCoordinator.SEGMENT_COMPARATOR).onResultOf(Pair.<DataSegment>rhs())
      );
      Arrays.fill(totalSegmentsPerServerInDs, 0);

      final int numSegmentsInDS = allSegmentsInDS.size();
      final int groupPerServer = Math.max(1, numSegmentsInDS / serverCount / DS_GROUP_NUM);
      final int tolerance = (int) (groupPerServer * DS_GROUP_TOLERANCE_RATIO);

      int i = 0;
      for (int group = 1; found.size() < maxSegmentsToMove && i < numSegmentsInDS; group++) {
        for (int x = 0; x < serverCount; x++) {
          segmentsPerServer[x].clear();
        }
        excessive.clear();
        deficit.clear();

        int limit = Math.min(numSegmentsInDS, i + groupPerServer * serverCount);
        for (; i < limit; i++) {
          Pair<Integer, DataSegment> pair = allSegmentsInDS.get(i);
          segmentsPerServer[pair.lhs].add(pair.rhs);
          totalSegmentsPerServerInDs[pair.lhs]++;
        }
        final int totalExpectedPerServer = i / serverCount;   // casting induces `excessive` rather than `deficits`
        for (int x = 0; x < serverCount; x++) {
          int count = totalSegmentsPerServerInDs[x];
          if (count < totalExpectedPerServer && !servers[x].isDecommissioned()) {
            for (; count < totalExpectedPerServer; count++) {
              deficit.add(x);
            }
          } else {
            for (; count > totalExpectedPerServer + tolerance; count--) {
              excessive.add(x);
            }
          }
        }
        if (!excessive.isEmpty() && deficit.isEmpty()) {
          final int idx = findServerBelowBaseLine(totalSegmentsPerServer, baseLine);
          if (idx >= 0) {
            deficit.add(idx);
          }
        }

        if (excessive.isEmpty() || deficit.isEmpty()) {
          continue;
        }

        excessive.shuffle(random);
        deficit.shuffle(random);

        for (final int from : excessive) {
          for (int x = 0; x < deficit.size(); x++) {
            final int to = deficit.get(x);
            if (to < 0 || from == to) {
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

            totalSegmentsPerServerInDs[from]--;
            totalSegmentsPerServerInDs[to]++;
            totalSegmentsPerServer[from]--;
            totalSegmentsPerServer[to]++;
            deficit.set(x, -1);
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

  private int findServerBelowBaseLine(final int[] totalSegmentsPerServer, final int minBaseLine)
  {
    int x = -1;
    for (int i = 0; i < totalSegmentsPerServer.length; i++) {
      if (totalSegmentsPerServer[i] > minBaseLine) {
        continue;
      }
      if (x < 0 || totalSegmentsPerServer[i] < totalSegmentsPerServer[x]) {
        x = i;
      }
    }
    return x;
  }

  @Override
  public ServerHolder findNewSegmentHomeReplicator(DataSegment proposalSegment, List<ServerHolder> serverHolders)
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
