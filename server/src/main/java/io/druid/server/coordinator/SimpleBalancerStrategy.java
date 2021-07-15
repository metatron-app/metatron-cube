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
import io.druid.client.ImmutableDruidDataSource;
import io.druid.collections.IntList;
import io.druid.common.DateTimes;
import io.druid.common.IntTagged;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.granularity.PeriodGranularity;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.coordinator.helper.DruidCoordinatorBalancer;
import io.druid.timeline.DataSegment;
import org.joda.time.Period;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * don't bother tier, capacity, etc. just simple balancing of segments.
 */
public class SimpleBalancerStrategy implements BalancerStrategy
{
  private static final Logger LOG = new Logger(SimpleBalancerStrategy.class);

  private static final Granularity DEFAULT_OFFSET_PERIOD = Granularities.DAY;
  private static final int DEFAULT_INIT_GROUPING = 96;

  private static final float BASELINE_RATIO = 0.95f;
  private static final float EXCESSIVE_TOLERANCE_RATIO = 0.1f;

  private final Granularity offset;
  private final int initialGrouping;
  private final Random random = new Random();

  public SimpleBalancerStrategy(Period offsetPeriod, Integer initialGrouping)
  {
    this.offset = offsetPeriod == null ? DEFAULT_OFFSET_PERIOD : new PeriodGranularity(offsetPeriod, null, null);
    this.initialGrouping = initialGrouping == null || initialGrouping <= 0 ? DEFAULT_INIT_GROUPING : initialGrouping;
  }

  @Override
  public int balance(
      final List<ServerHolder> serverHolders,
      final DruidCoordinatorBalancer balancer,
      final DruidCoordinatorRuntimeParams params
  )
  {
    if (serverHolders.isEmpty()) {
      return 0;
    }
    int numberOfQueuedSegments = 0;
    for (ServerHolder holder : serverHolders) {
      numberOfQueuedSegments += holder.getPeon().getNumberOfQueuedSegments();
    }
    final int segmentsToMove = params.getMaxSegmentsToMove() - numberOfQueuedSegments;
    if (segmentsToMove <= 0) {
      return 0;
    }
    final long start = offset.bucketStart(DateTimes.nowUtc()).getMillis();

    final Set<String> dataSourceNames = Sets.newTreeSet();  // sorted to estimate progress
    final ServerHolder[] holders = serverHolders.toArray(new ServerHolder[0]);
    final int[] totalSegments = new int[holders.length];
    int numTotalSegments = 0;
    for (int i = 0; i < holders.length; i++) {
      totalSegments[i] = holders[i].getServer().getSegments().size();
      numTotalSegments += totalSegments[i];
      Iterables.addAll(dataSourceNames, holders[i].getServer().getDataSourceNames());
    }
    final int serverCount = holders.length;
    final int baseLine = (int) (BASELINE_RATIO * numTotalSegments / serverCount);

    // per DS, incremental
    final int[] totalSegmentsPerDs = new int[serverCount];

    // per group
    final IntList deficit = new IntList();
    final IntList excessive = new IntList();

    @SuppressWarnings("unchecked")
    final List<DataSegment>[] totalSegmentsPerGroup = (List<DataSegment>[]) Array.newInstance(List.class, serverCount);
    for (int x = 0; x < serverCount; x++) {
      totalSegmentsPerGroup[x] = Lists.newArrayList();
    }

    int balanced = 0;
    for (String dataSourceName : dataSourceNames) {
      List<IntTagged<DataSegment>> allSegmentsInDS = Lists.newArrayList();
      for (int i = 0; i < serverCount; i++) {
        ImmutableDruidDataSource dataSource = holders[i].getServer().getDataSource(dataSourceName);
        if (dataSource != null) {
          for (DataSegment segment : dataSource.getSegments()) {
            if (segment.getInterval().getEndMillis() <= start && !holders[i].isDroppingSegment(segment)) {
              allSegmentsInDS.add(IntTagged.of(i, segment));
            }
          }
        }
      }
      Collections.sort(
          allSegmentsInDS, Ordering.from(DataSegment.TIME_DESCENDING).onResultOf(IntTagged::value)
      );
      Arrays.fill(totalSegmentsPerDs, 0);

      final int numSegmentsInDS = allSegmentsInDS.size();
      final int firstGroupSize = Math.max(serverCount, numSegmentsInDS / initialGrouping);

      int i = 0;
      while (segmentsToMove - balanced > 0 && i < numSegmentsInDS) {
        for (int x = 0; x < serverCount; x++) {
          totalSegmentsPerGroup[x].clear();
        }
        excessive.clear();
        deficit.clear();

        final int limit = Math.min(numSegmentsInDS, i + (i == 0 ? firstGroupSize : i << 1));
        for (; i < limit; i++) {
          IntTagged<DataSegment> pair = allSegmentsInDS.get(i);
          totalSegmentsPerGroup[pair.tag].add(pair.value);
          totalSegmentsPerDs[pair.tag]++;
        }
        final int deficitThreshold = i / serverCount;   // casting induces `excessive` rather than `deficits`
        final float excessiveThreshold = deficitThreshold + EXCESSIVE_TOLERANCE_RATIO * i / serverCount;
        for (int x = 0; x < serverCount; x++) {
          int count = totalSegmentsPerDs[x];
          if (count < deficitThreshold && !holders[x].isDecommissioned()) {
            for (; count < deficitThreshold; count++) {
              deficit.add(x);
            }
          } else {
            for (; count > excessiveThreshold; count--) {
              excessive.add(x);
            }
          }
        }
        if (excessive.isEmpty()) {
          continue;
        }
        excessive.shuffle(random);
        deficit.shuffle(random);

        if (deficit.isEmpty()) {
          int expected = Math.max(1, excessive.size() >> 2);
          deficit.addAll(serversBelowBaseLine(holders, totalSegments, baseLine, expected));
        }

        int remain = deficit.size();
        for (int x = 0; remain > 0 && x < excessive.size(); x++) {
          final int from = excessive.get(x);
          for (int y = 0; remain > 0 && y < deficit.size(); y++) {
            final int to = deficit.get(y);
            if (to < 0 || from == to) {
              continue;
            }
            final int index = chooseSegmentToMove(totalSegmentsPerGroup[from], holders[to]);
            if (index < 0) {
              continue;
            }
            final DataSegment segment = totalSegmentsPerGroup[from].get(index);
            if (balancer.moveSegment(segment, holders[from], holders[to])) {
              balanced++;
              totalSegmentsPerGroup[from].set(index, null);
              totalSegmentsPerDs[from]--;
              totalSegmentsPerDs[to]++;
              totalSegments[from]--;
              totalSegments[to]++;
              deficit.set(y, -1);
              remain--;
            }
          }
        }
      }
    }
    return balanced;
  }

  private IntStream serversBelowBaseLine(ServerHolder[] servers, int[] segmentsPerServer, int baseLine, int expected)
  {
    final List<int[]> taggedSegmentsPerServer = Lists.newArrayList();
    for (int i = 0; i < segmentsPerServer.length; i++) {
      if (!servers[i].isDecommissioned() && segmentsPerServer[i] <= baseLine) {
        taggedSegmentsPerServer.add(new int[]{i, segmentsPerServer[i]});
      }
    }
    if (taggedSegmentsPerServer.isEmpty()) {
      return IntStream.empty();
    }
    Collections.sort(taggedSegmentsPerServer, (a, b) -> Integer.compare(a[1], b[1]));
    return taggedSegmentsPerServer.stream().mapToInt(x -> x[0]).limit(expected);
  }

  private int chooseSegmentToMove(List<DataSegment> segments, ServerHolder server)
  {
    final long available = server.getAvailableSize();
    for (int i = segments.size() - 1; i >= 0; i--) {
      final DataSegment segment = segments.get(i);
      if (segment == null) {
        continue;
      }
      if (segment.getSize() >= available || server.isServingSegment(segment) || server.isLoadingSegment(segment)) {
        continue;
      }
      return i;
    }
    return -1;
  }

  @Override
  public ServerHolder findNewSegmentHomeReplicator(DataSegment segment, List<ServerHolder> holders)
  {
    // can be used for bulk loading when server is down or decommisioned.. need to handle that properly
    int minExpectedSegments = -1;
    ServerHolder minServer = null;
    for (ServerHolder holder : RandomBalancerStrategy.filter(segment, holders, false)) {
      int expectedSegments = holder.getNumExpectedSegments();
      if (minExpectedSegments < 0 || minExpectedSegments > expectedSegments) {
        minExpectedSegments = expectedSegments;
        minServer = holder;
      }
    }
    return minServer;
  }
}
