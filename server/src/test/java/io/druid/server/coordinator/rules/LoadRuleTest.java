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

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.DruidServer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.core.LoggingEmitter;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.CostBalancerStrategyFactory;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.SegmentReplicantLookup;
import io.druid.server.coordinator.ServerHolder;
import io.druid.server.coordinator.SimpleBalancerStrategy;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 */
public class LoadRuleTest
{
  private static final Logger log = new Logger(LoadRuleTest.class);
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private static final ServiceEmitter emitter = new ServiceEmitter(
      "service",
      "host",
      new LoggingEmitter(
          log,
          LoggingEmitter.Level.ERROR,
          jsonMapper
      )
  );

  private DruidCoordinator coordinator;
  private LoadQueuePeon mockPeon;
  private DataSegment segment;


  @Before
  public void setUp() throws Exception
  {
    EmittingLogger.registerEmitter(emitter);
    emitter.start();
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    EasyMock.expect(coordinator.isAvailable(EasyMock.anyObject())).andReturn(true).anyTimes();
    EasyMock.expect(coordinator.getFailedServers(EasyMock.<DataSegment>anyObject())).andReturn(ImmutableSet.of())
            .anyTimes();
    EasyMock.replay(coordinator);

    mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    segment = new DataSegment(
        "foo",
        new Interval("0/3000"),
        new DateTime().toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        0
    );
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testLoad() throws Exception
  {
    mockPeon.loadSegment(
        EasyMock.<DataSegment>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.isLoadingSegment(EasyMock.<DataSegment>anyObject())).andReturn(false).atLeastOnce();
    EasyMock.expect(mockPeon.getNumSegmentsToLoad()).andReturn(0).atLeastOnce();
    EasyMock.expect(mockPeon.getNumSegmentsToDrop()).andReturn(0).atLeastOnce();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    LoadRule rule = new LoadRule()
    {
      private final Map<String, Integer> tiers = ImmutableMap.of(
          "hot", 1,
          DruidServer.DEFAULT_TIER, 2
      );

      @Override
      public Map<String, Integer> getTieredReplicants()
      {
        return tiers;
      }

      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
      {
        return true;
      }
    };

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverHot",
                            "hostHot",
                            1000,
                            "historical",
                            "hot",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            DruidServer.DEFAULT_TIER,
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            DruidServer.DEFAULT_TIER,
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy = new SimpleBalancerStrategy(null, null, null, null);
//            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withCoordinatorStats(new CoordinatorStats())
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withBalancerStrategy(balancerStrategy)
                                     .withAvailableSegments(Arrays.asList(segment)).build();
    rule.run(coordinator, params, segment);

    CoordinatorStats stats = params.getCoordinatorStats();
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").getLong("hot") == 1);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").getLong(DruidServer.DEFAULT_TIER) == 2);
    exec.shutdown();
  }

  @Test
  public void testDrop() throws Exception
  {
    mockPeon.dropSegment(
        EasyMock.<DataSegment>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().atLeastOnce();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.replay(mockPeon);

    LoadRule rule = new LoadRule()
    {
      private final Map<String, Integer> tiers = ImmutableMap.of(
          "hot", 0,
          DruidServer.DEFAULT_TIER, 0
      );

      @Override
      public Map<String, Integer> getTieredReplicants()
      {
        return tiers;
      }

      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
      {
        return true;
      }
    };

    DruidServer server1 = new DruidServer(
        "serverHot",
        "hostHot",
        1000,
        "historical",
        "hot",
        0
    );
    server1.addDataSegment(segment);
    DruidServer server2 = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        DruidServer.DEFAULT_TIER,
        0
    );
    server2.addDataSegment(segment);
    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            DruidServer.DEFAULT_TIER,
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server2.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withCoordinatorStats(new CoordinatorStats())
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withBalancerStrategy(balancerStrategy)
                                     .withAvailableSegments(Arrays.asList(segment)).build();
    rule.run(coordinator, params, segment);

    CoordinatorStats stats = params.getCoordinatorStats();
    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").getLong("hot") == 1);
    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").getLong(DruidServer.DEFAULT_TIER) == 1);
    exec.shutdown();
  }

  @Test
  public void testLoadWithNonExistentTier() throws Exception
  {
    mockPeon.loadSegment(
        EasyMock.<DataSegment>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.isLoadingSegment(EasyMock.<DataSegment>anyObject())).andReturn(false).atLeastOnce();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    LoadRule rule = new LoadRule()
    {
      private final Map<String, Integer> tiers = ImmutableMap.of(
          "nonExistentTier", 1,
          "hot", 1
      );

      @Override
      public Map<String, Integer> getTieredReplicants()
      {
        return tiers;
      }

      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
      {
        return true;
      }
    };

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverHot",
                            "hostHot",
                            1000,
                            "historical",
                            "hot",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withCoordinatorStats(new CoordinatorStats())
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withBalancerStrategy(balancerStrategy)
                                     .withAvailableSegments(Arrays.asList(segment)).build();
    rule.run(coordinator, params, segment);

    CoordinatorStats stats = params.getCoordinatorStats();
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").getLong("hot") == 1);
    exec.shutdown();
  }

  @Test
  public void testDropWithNonExistentTier() throws Exception
  {
    mockPeon.dropSegment(
        EasyMock.<DataSegment>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().atLeastOnce();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.replay(mockPeon);

    LoadRule rule = new LoadRule()
    {
      private final Map<String, Integer> tiers = ImmutableMap.of(
          "nonExistentTier", 1,
          "hot", 1
      );

      @Override
      public Map<String, Integer> getTieredReplicants()
      {
        return tiers;
      }

      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
      {
        return true;
      }
    };

    DruidServer server1 = new DruidServer(
        "serverHot",
        "hostHot",
        1000,
        "historical",
        "hot",
        0
    );
    DruidServer server2 = new DruidServer(
        "serverHo2t",
        "hostHot2",
        1000,
        "historical",
        "hot",
        0
    );
    server1.addDataSegment(segment);
    server2.addDataSegment(segment);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1.toImmutableDruidServer(),
                        mockPeon
                    ),
                    new ServerHolder(
                        server2.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withCoordinatorStats(new CoordinatorStats())
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withBalancerStrategy(balancerStrategy)
                                     .withAvailableSegments(Arrays.asList(segment)).build();
    rule.run(coordinator, params, segment);

    CoordinatorStats stats = params.getCoordinatorStats();
    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").getLong("hot") == 1);
    exec.shutdown();
  }
}
