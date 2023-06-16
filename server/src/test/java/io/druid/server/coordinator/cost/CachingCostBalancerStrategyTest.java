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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.DruidServer;
import io.druid.common.DateTimes;
import io.druid.concurrent.Execs;
import io.druid.server.coordinator.CachingCostBalancerStrategy;
import io.druid.server.coordinator.CostBalancerStrategy;
import io.druid.server.coordinator.LoadQueuePeonTester;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CachingCostBalancerStrategyTest
{
  private static final int DAYS_IN_MONTH = 30;
  private static final int SEGMENT_SIZE = 100;
  private static final int NUMBER_OF_SEGMENTS_ON_SERVER = 10000;
  private static final int NUMBER_OF_QUERIES = 1000;
  private static final int NUMBER_OF_SERVERS = 3;

  private List<ServerHolder> serverHolderList;
  private List<DataSegment> segmentQueries;
  private ListeningExecutorService executorService;

  @Before
  public void setUp() throws Exception
  {
    Random random = new Random(0);
    DateTime referenceTime = DateTimes.utc("2014-01-01T00:00:00");

    serverHolderList = Lists.<ServerHolder>newArrayList();
    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      serverHolderList.add(
          createServerHolder(
              String.valueOf(i),
              String.valueOf(i),
              SEGMENT_SIZE * (NUMBER_OF_SEGMENTS_ON_SERVER + NUMBER_OF_QUERIES),
              NUMBER_OF_SEGMENTS_ON_SERVER,
              random,
              referenceTime
          )
      );
    }

    segmentQueries = createDataSegments(NUMBER_OF_QUERIES, random, referenceTime);
    executorService = MoreExecutors.listeningDecorator(Execs.singleThreaded(""));
  }

  @After
  public void tearDown() throws Exception
  {
    executorService.shutdownNow();
  }

  @Test
  public void decisionTest() throws Exception
  {
    CachingCostBalancerStrategy cachingCostBalancerStrategy = createCachingCostBalancerStrategy(
        serverHolderList,
        executorService
    );
    CostBalancerStrategy costBalancerStrategy = createCostBalancerStrategy(executorService);
    int notEqual = 0;
    for (DataSegment s : segmentQueries) {
      ServerHolder s1 = cachingCostBalancerStrategy.findNewSegmentHomeBalancer(s, serverHolderList, -1);
      ServerHolder s2 = costBalancerStrategy.findNewSegmentHomeBalancer(s, serverHolderList, -1);
      if (!(s1.getServer().getName().equals(s2.getServer().getName()))) {
        notEqual += 1;
      }
    }
    Assert.assertTrue(((double) notEqual / (double) segmentQueries.size()) < 0.01);
  }

  private CachingCostBalancerStrategy createCachingCostBalancerStrategy(
      List<ServerHolder> serverHolders,
      ListeningExecutorService listeningExecutorService
  )
  {
    ClusterCostCache.Builder builder = ClusterCostCache.builder();
    for (ServerHolder server : serverHolders) {
      for (DataSegment segment : server.getServer().getSegments().values()) {
        builder.addSegment(server.getServer().getName(), segment);
      }
    }
    return new CachingCostBalancerStrategy(builder.build(), listeningExecutorService);
  }

  private CostBalancerStrategy createCostBalancerStrategy(ListeningExecutorService listeningExecutorService)
  {
    return new CostBalancerStrategy(listeningExecutorService);
  }

  private ServerHolder createServerHolder(
      String name,
      String host,
      int maxSize,
      int numberOfSegments,
      Random random,
      DateTime referenceTime
  )
  {
    DruidServer druidServer = new DruidServer(name, host, maxSize, "historical", "normal", 0);
    for (DataSegment segment : createDataSegments(numberOfSegments, random, referenceTime)) {
      druidServer.addDataSegment(segment);
    }
    return new ServerHolder(druidServer.toImmutableDruidServer(), new LoadQueuePeonTester());
  }

  private List<DataSegment> createDataSegments(
      int numberOfSegments,
      Random random,
      DateTime referenceTime
  )
  {
    Set<DataSegment> segmentSet = Sets.newHashSet();
    for (int i = 0; i < numberOfSegments; i++) {
      segmentSet.add(createRandomSegment(random, referenceTime));
    }
    return Lists.newArrayList(segmentSet);
  }

  private DataSegment createRandomSegment(Random random, DateTime referenceTime)
  {
    int timeShift = random.nextInt((int) TimeUnit.DAYS.toHours(DAYS_IN_MONTH * 12));
    return new DataSegment(
        String.valueOf(random.nextInt(50)),
        new Interval(referenceTime.plusHours(timeShift), referenceTime.plusHours(timeShift + 1)),
        "version",
        Collections.<String, Object>emptyMap(),
        Collections.<String>emptyList(),
        Collections.<String>emptyList(),
        null,
        0,
        100
    );
  }
}
