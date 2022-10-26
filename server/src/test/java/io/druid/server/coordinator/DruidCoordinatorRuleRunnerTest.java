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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.DruidServer;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceEventBuilder;
import io.druid.metadata.MetadataRuleManager;
import io.druid.segment.IndexIO;
import io.druid.server.coordinator.helper.DruidCoordinatorRuleRunner;
import io.druid.server.coordinator.rules.ForeverLoadRule;
import io.druid.server.coordinator.rules.IntervalDropRule;
import io.druid.server.coordinator.rules.IntervalLoadRule;
import io.druid.server.coordinator.rules.Rule;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

/**
 */
public class DruidCoordinatorRuleRunnerTest
{
  private DruidCoordinator coordinator;
  private LoadQueuePeon mockPeon;
  private List<DataSegment> availableSegments;
  private DruidCoordinatorRuleRunner ruleRunner;
  private ServiceEmitter emitter;
  private MetadataRuleManager databaseRuleManager;

  @Before
  public void setUp()
  {
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    EasyMock.expect(coordinator.getBlacklisted(true)).andReturn(ImmutableSet.of()).anyTimes();
    EasyMock.expect(coordinator.getFailedServers(EasyMock.<DataSegment>anyObject())).andReturn(ImmutableSet.of())
            .anyTimes();

    mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    databaseRuleManager = EasyMock.createMock(MetadataRuleManager.class);

    DateTime start = new DateTime("2012-01-01");
    availableSegments = Lists.newArrayList();
    for (int i = 0; i < 24; i++) {
      availableSegments.add(
          new DataSegment(
              "test",
              new Interval(start, start.plusHours(1)),
              new DateTime().toString(),
              Maps.<String, Object>newHashMap(),
              Lists.<String>newArrayList(),
              Lists.<String>newArrayList(),
              NoneShardSpec.instance(),
              IndexIO.CURRENT_VERSION_ID,
              1
          )
      );
      start = start.plusHours(1);
    }

    ruleRunner = new DruidCoordinatorRuleRunner(coordinator);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(coordinator);
    EasyMock.verify(databaseRuleManager);
  }

  /**
   * Nodes:
   * hot - 1 replicant
   * normal - 1 replicant
   * cold - 1 replicant
   *
   * @throws Exception
   */
  @Test
  public void testRunThreeTiersOneReplicant() throws Exception
  {
    mockCoordinator();
    expectLoad();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 1)),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1)),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), ImmutableMap.<String, Integer>of("cold", 1))
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

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
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            "normal",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            "cold",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverCold",
                            "hostCold",
                            1000,
                            "historical",
                            "cold",
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
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .withBalancerStrategy(balancerStrategy)
            .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(5).build())
            .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(6L, stats.getPerTierStats().get("assignedCount").getLong("hot"));
    Assert.assertEquals(6L, stats.getPerTierStats().get("assignedCount").getLong("normal"));
    Assert.assertEquals(12L, stats.getPerTierStats().get("assignedCount").getLong("cold"));
    Assert.assertNull(stats.getPerTierStats().get("unassignedCount"));
    Assert.assertNull(stats.getPerTierStats().get("unassignedSize"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  private void expectLoad()
  {
    mockPeon.loadSegment(
        EasyMock.<DataSegment>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject(),
        EasyMock.<Predicate<DataSegment>>anyObject()
    );
    EasyMock.expectLastCall().andReturn(true).atLeastOnce();
    EasyMock.expect(mockPeon.isLoadingSegment(EasyMock.<DataSegment>anyObject())).andReturn(false).atLeastOnce();
  }

  private void expectDrop()
  {
    mockPeon.dropSegment(
        EasyMock.<DataSegment>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject(),
        EasyMock.<Predicate<DataSegment>>anyObject()
    );
    EasyMock.expectLastCall().andReturn(true).atLeastOnce();
  }

  /**
   * Nodes:
   * hot - 2 replicants
   * cold - 1 replicant
   *
   * @throws Exception
   */
  @Test
  public void testRunTwoTiersTwoReplicants() throws Exception
  {
    mockCoordinator();
    expectLoad();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 2)),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), ImmutableMap.<String, Integer>of("cold", 1))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

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
                    ),
                    new ServerHolder(
                        new DruidServer(
                            "serverHot2",
                            "hostHot2",
                            1000,
                            "historical",
                            "hot",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            "cold",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverCold",
                            "hostCold",
                            1000,
                            "historical",
                            "cold",
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
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .withBalancerStrategy(balancerStrategy)
            .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(12L, stats.getPerTierStats().get("assignedCount").getLong("hot"));
    Assert.assertEquals(18L, stats.getPerTierStats().get("assignedCount").getLong("cold"));
    Assert.assertNull(stats.getPerTierStats().get("unassignedCount"));
    Assert.assertNull(stats.getPerTierStats().get("unassignedSize"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  /**
   * Nodes:
   * hot - 1 replicant
   * normal - 1 replicant
   *
   * @throws Exception
   */
  @Test
  public void testRunTwoTiersWithExistingSegments() throws Exception
  {
    mockCoordinator();
    expectLoad();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 1)),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer normServer = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment availableSegment : availableSegments) {
      normServer.addDataSegment(availableSegment);
    }

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
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        normServer.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(segmentReplicantLookup)
            .withBalancerStrategy(balancerStrategy)
            .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(12L, stats.getPerTierStats().get("assignedCount").getLong("hot"));
    Assert.assertEquals(0L, stats.getPerTierStats().get("assignedCount").getLong("normal"));
    Assert.assertNull(stats.getPerTierStats().get("unassignedCount"));
    Assert.assertNull(stats.getPerTierStats().get("unassignedSize"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunTwoTiersTierDoesNotExist() throws Exception
  {
    mockCoordinator();
    expectLoad();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("hot",1)),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            "normal",
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
        new DruidCoordinatorRuntimeParams.Builder()
            .withEmitter(emitter)
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .withBalancerStrategy(balancerStrategy)
            .build();

    ruleRunner.run(params);

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunRuleDoesNotExist() throws Exception
  {
    mockCoordinator();
    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(emitter);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-02T00:00:00.000Z/2012-01-03T00:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            "normal",
                            0
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withEmitter(emitter)
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .withCoordinatorStats(new CoordinatorStats())
            .build();

    ruleRunner.run(params);

    EasyMock.verify(emitter);
  }

  @Test
  public void testDropRemove() throws Exception
  {
    expectDrop();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(coordinator.getDynamicConfigs()).andReturn(
        new CoordinatorDynamicConfig(
            0, 0, 0, 0, 0, 1, 24, 0, false, null, null, null
        )
    ).anyTimes();
    EasyMock.expect(coordinator.disableSegment(EasyMock.<String>anyObject(), EasyMock.<DataSegment>anyObject())).andReturn(true).atLeastOnce();
    EasyMock.replay(coordinator);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1)),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment segment : availableSegments) {
      server.addDataSegment(segment);
    }

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(12L, stats.getGlobalStats().getLong("deletedCount"));

    exec.shutdown();
    EasyMock.verify(coordinator);
  }

  @Test
  public void testDropTooManyInSameTier() throws Exception
  {
    mockCoordinator();
    expectDrop();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 1)),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        "normal",
        0
    );
    server1.addDataSegment(availableSegments.get(0));

    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment segment : availableSegments) {
      server2.addDataSegment(segment);
    }

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(1L, stats.getPerTierStats().get("droppedCount").getLong("normal"));
    Assert.assertEquals(12L, stats.getGlobalStats().getLong("deletedCount"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropTooManyInDifferentTiers() throws Exception
  {
    mockCoordinator();
    expectLoad();
    expectDrop();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 1)),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "server1",
        "host1",
        1000,
        "historical",
        "hot",
        0
    );
    server1.addDataSegment(availableSegments.get(0));
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment segment : availableSegments) {
      server2.addDataSegment(segment);
    }

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
            "normal",
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(1L, stats.getPerTierStats().get("droppedCount").getLong("normal"));
    Assert.assertEquals(12L, stats.getGlobalStats().getLong("deletedCount"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDontDropInDifferentTiers() throws Exception
  {
    mockCoordinator();
    expectLoad();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), ImmutableMap.<String, Integer>of("hot", 1)),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "server1",
        "host1",
        1000,
        "historical",
        "hot",
        0
    );
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal",
        0
    );
    for (DataSegment segment : availableSegments) {
      server2.addDataSegment(segment);
    }
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
            "normal",
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertNull(stats.getPerTierStats().get("droppedCount"));
    Assert.assertEquals(12L, stats.getGlobalStats().getLong("deletedCount"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropServerActuallyServesSegment() throws Exception
  {
    mockCoordinator();
    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T01:00:00.000Z"), ImmutableMap.<String, Integer>of("normal", 0))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "server1",
        "host1",
        1000,
        "historical",
        "normal",
        0
    );
    server1.addDataSegment(availableSegments.get(0));
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal",
        0
    );
    server2.addDataSegment(availableSegments.get(1));
    DruidServer server3 = new DruidServer(
        "serverNorm3",
        "hostNorm3",
        1000,
        "historical",
        "normal",
        0
    );
    server3.addDataSegment(availableSegments.get(1));
    server3.addDataSegment(availableSegments.get(2));

    expectDrop();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    LoadQueuePeon anotherMockPeon = EasyMock.createMock(LoadQueuePeon.class);
    anotherMockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(anotherMockPeon.getLoadQueueSize()).andReturn(10L).atLeastOnce();
    EasyMock.replay(anotherMockPeon);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1.toImmutableDruidServer(),
                        mockPeon
                    ),
                    new ServerHolder(
                        server2.toImmutableDruidServer(),
                        anotherMockPeon
                    ),
                    new ServerHolder(
                        server3.toImmutableDruidServer(),
                        anotherMockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params = new DruidCoordinatorRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withCoordinatorStats(new CoordinatorStats())
        .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMillisToWaitBeforeDeleting(0L).build())
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .withBalancerStrategy(balancerStrategy)
        .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(1L, stats.getPerTierStats().get("droppedCount").getLong("normal"));

    exec.shutdown();
    EasyMock.verify(mockPeon);
    EasyMock.verify(anotherMockPeon);
  }

  @Test
  public void testRulesRunOnNonOvershadowedSegmentsOnly() throws Exception
  {
    DataSegment v1 = new DataSegment(
        "test",
        new Interval("2012-01-01/2012-01-02"),
        "1",
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );
    DataSegment v2 = new DataSegment(
        "test",
        new Interval("2012-01-01/2012-01-02"),
        "2",
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );
    Map<String, List<DataSegment>> availableSegments = ImmutableMap.of("test", Arrays.asList(v1, v2));

    mockCoordinator();
    mockPeon.loadSegment(
        EasyMock.eq(v2), EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject(), EasyMock.<Predicate<DataSegment>>anyObject()
    );
    EasyMock.expectLastCall().andReturn(true).atLeastOnce();
    EasyMock.expect(mockPeon.isLoadingSegment(EasyMock.<DataSegment>anyObject())).andReturn(false).atLeastOnce();
    mockPeon.getSegmentsToLoad(EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new ForeverLoadRule(ImmutableMap.of(DruidServer.DEFAULT_TIER, 1))
        )).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            DruidServer.DEFAULT_TIER,
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverHot",
                            "hostHot",
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
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        new DruidCoordinatorRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(Suppliers.ofInstance(availableSegments))
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .withBalancerStrategy(balancerStrategy)
            .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(5).build())
            .build();

    DruidCoordinatorRuntimeParams afterParams = ruleRunner.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();

    Assert.assertEquals(1, stats.getPerTierStats().get("assignedCount").size());
    Assert.assertEquals(1, stats.getPerTierStats().get("assignedCount").getLong("_default_tier"));
    Assert.assertNull(stats.getPerTierStats().get("unassignedCount"));
    Assert.assertNull(stats.getPerTierStats().get("unassignedSize"));

    Assert.assertEquals(2, Iterables.size(Iterables.concat(availableSegments.values())));
    Assert.assertEquals(availableSegments, params.getAvailableSegments());
    Assert.assertEquals(availableSegments, afterParams.getAvailableSegments());

    EasyMock.verify(mockPeon);
    exec.shutdown();
  }

  private void mockCoordinator()
  {
    EasyMock.expect(coordinator.isAvailable(EasyMock.anyObject())).andReturn(true).anyTimes();
    EasyMock.expect(coordinator.getDynamicConfigs()).andReturn(
        new CoordinatorDynamicConfig(
            0, 0, 0, 0, 0, 1, 24, 0, false, null, null, null
        )
    ).anyTimes();
    EasyMock.expect(coordinator.disableSegment(EasyMock.<String>anyObject(), EasyMock.<DataSegment>anyObject())).andReturn(true).anyTimes();
    EasyMock.replay(coordinator);
  }
}
