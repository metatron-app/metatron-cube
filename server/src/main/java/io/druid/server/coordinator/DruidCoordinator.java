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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.ServerInventoryView;
import io.druid.client.ServerView;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.collections.CountingMap;
import io.druid.common.config.JacksonConfigManager;
import io.druid.common.utils.JodaUtils;
import io.druid.concurrent.Execs;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.data.KeyedData.StringKeyed;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.CoordinatorIndexingServiceHelper;
import io.druid.guice.annotations.Self;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.helper.DruidCoordinatorBalancer;
import io.druid.server.coordinator.helper.DruidCoordinatorCleanupOvershadowed;
import io.druid.server.coordinator.helper.DruidCoordinatorCleanupUnneeded;
import io.druid.server.coordinator.helper.DruidCoordinatorHelper;
import io.druid.server.coordinator.helper.DruidCoordinatorLogger;
import io.druid.server.coordinator.helper.DruidCoordinatorRuleRunner;
import io.druid.server.coordinator.helper.DruidCoordinatorSegmentInfoLoader;
import io.druid.server.coordinator.rules.ForeverLoadRule;
import io.druid.server.coordinator.rules.LoadRule;
import io.druid.server.coordinator.rules.Rule;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@ManageLifecycle
public class DruidCoordinator
{
  public static final String COORDINATOR_OWNER_NODE = "_COORDINATOR";

  public static Comparator<DataSegment> SEGMENT_COMPARATOR = Ordering.from(JodaUtils.intervalsByEndThenStart())
                                                                     .onResultOf(
                                                                         new Function<DataSegment, Interval>()
                                                                         {
                                                                           @Override
                                                                           public Interval apply(DataSegment segment)
                                                                           {
                                                                             return segment.getInterval();
                                                                           }
                                                                         })
                                                                     .compound(Ordering.<DataSegment>natural())
                                                                     .reverse();

  private static final EmittingLogger log = new EmittingLogger(DruidCoordinator.class);
  private final Object lock = new Object();
  private final DruidCoordinatorConfig config;
  private final ZkPathsConfig zkPaths;
  private final JacksonConfigManager configManager;
  private final MetadataSegmentManager metadataSegmentManager;
  private final ServerInventoryView<?> serverInventoryView;
  private final MetadataRuleManager metadataRuleManager;
  private final CuratorFramework curator;
  private final ServiceEmitter emitter;
  private final IndexingServiceClient indexingServiceClient;
  private final ScheduledExecutorService exec;
  private final LoadQueueTaskMaster taskMaster;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final AtomicReference<LeaderLatch> leaderLatch;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DruidNode self;
  private final Set<DruidCoordinatorHelper> indexingServiceHelpers;

  private volatile boolean started = false;
  private volatile int leaderCounter = 0;
  private volatile boolean leader = false;
  private volatile DruidCoordinatorRuntimeParams prevParam;

  private final CoordinatorDynamicConfig defaultConfig;
  private final BalancerStrategyFactory factory;
  private final ListeningExecutorService balancerExec;

  private final ReplicationThrottler replicatorThrottler;

  @Inject
  public DruidCoordinator(
      DruidCoordinatorConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      MetadataSegmentManager metadataSegmentManager,
      ServerInventoryView serverInventoryView,
      MetadataRuleManager metadataRuleManager,
      CuratorFramework curator,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      IndexingServiceClient indexingServiceClient,
      LoadQueueTaskMaster taskMaster,
      ServiceAnnouncer serviceAnnouncer,
      @Self DruidNode self,
      @CoordinatorIndexingServiceHelper Set<DruidCoordinatorHelper> indexingServiceHelpers,
      CoordinatorDynamicConfig defaultConfig,
      BalancerStrategyFactory factory
  )
  {
    this(
        config,
        zkPaths,
        configManager,
        metadataSegmentManager,
        serverInventoryView,
        metadataRuleManager,
        curator,
        emitter,
        scheduledExecutorFactory,
        indexingServiceClient,
        taskMaster,
        serviceAnnouncer,
        self,
        Maps.<String, LoadQueuePeon>newConcurrentMap(),
        indexingServiceHelpers,
        defaultConfig,
        factory
    );
  }

  DruidCoordinator(
      DruidCoordinatorConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      MetadataSegmentManager metadataSegmentManager,
      ServerInventoryView serverInventoryView,
      MetadataRuleManager metadataRuleManager,
      CuratorFramework curator,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      IndexingServiceClient indexingServiceClient,
      LoadQueueTaskMaster taskMaster,
      ServiceAnnouncer serviceAnnouncer,
      DruidNode self,
      ConcurrentMap<String, LoadQueuePeon> loadQueuePeonMap,
      Set<DruidCoordinatorHelper> indexingServiceHelpers,
      CoordinatorDynamicConfig defaultConfig,
      BalancerStrategyFactory factory
  )
  {
    this.config = config;
    this.zkPaths = zkPaths;
    this.configManager = configManager;

    this.metadataSegmentManager = metadataSegmentManager;
    this.serverInventoryView = serverInventoryView;
    this.metadataRuleManager = metadataRuleManager;
    this.curator = curator;
    this.emitter = emitter;
    this.indexingServiceClient = indexingServiceClient;
    this.taskMaster = taskMaster;
    this.serviceAnnouncer = serviceAnnouncer;
    this.self = self;
    this.indexingServiceHelpers = indexingServiceHelpers;

    this.exec = scheduledExecutorFactory.create(1, "Coordinator-Exec--%d");

    this.leaderLatch = new AtomicReference<>(null);
    this.loadManagementPeons = loadQueuePeonMap;
    this.defaultConfig = defaultConfig == null ? new CoordinatorDynamicConfig() : defaultConfig;
    this.factory = factory;

    final CoordinatorDynamicConfig dynamicConfigs = getDynamicConfigs();
    this.balancerExec = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(dynamicConfigs.getBalancerComputeThreads())
    );
    this.replicatorThrottler = new ReplicationThrottler(
        dynamicConfigs.getReplicationThrottleLimit(),
        dynamicConfigs.getReplicantLifetime()
    );
    if (serverInventoryView.toString().contains("EasyMock")) {
      return;   // damn easy mock
    }
    serverInventoryView.registerServerCallback(
        exec,
        new ServerView.AbstractServerCallback()
        {
          @Override
          public ServerView.CallbackAction serverUpdated(DruidServer server)
          {
            if (leader && server.isDecommissioned()) {
              balanceNow();
            }
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  public boolean isLeader()
  {
    return leader;
  }

  public DruidNode getSelf()
  {
    return self;
  }

  public Map<String, LoadQueuePeon> getLoadManagementPeons()
  {
    return loadManagementPeons;
  }

  public Map<String, CountingMap<String>> getReplicationStatus()
  {
    final Map<String, CountingMap<String>> retVal = Maps.newHashMap();
    if (prevParam == null) {
      return retVal;
    }
    final SegmentReplicantLookup segmentReplicantLookup = prevParam.getSegmentReplicantLookup();

    final DateTime now = new DateTime();
    for (DataSegment segment : getAvailableDataSegments()) {
      List<Rule> rules = metadataRuleManager.getRulesWithDefault(segment.getDataSource());
      for (Rule rule : rules) {
        if (rule instanceof LoadRule && rule.appliesTo(segment, now)) {
          for (Map.Entry<String, Integer> entry : ((LoadRule) rule).getTieredReplicants().entrySet()) {
            CountingMap<String> dataSourceMap = retVal.get(entry.getKey());
            if (dataSourceMap == null) {
              dataSourceMap = new CountingMap<>();
              retVal.put(entry.getKey(), dataSourceMap);
            }

            int diff = Math.max(
                entry.getValue() - segmentReplicantLookup.getTotalReplicants(segment.getIdentifier(), entry.getKey()),
                0
            );
            dataSourceMap.add(segment.getDataSource(), diff);
          }
          break;
        }
      }
    }

    return retVal;
  }

  public CountingMap<String> getSegmentAvailability()
  {
    final CountingMap<String> retVal = new CountingMap<>();
    if (prevParam == null) {
      return retVal;
    }
    final SegmentReplicantLookup segmentReplicantLookup = prevParam.getSegmentReplicantLookup();

    for (DataSegment segment : getAvailableDataSegments()) {
      int available = (segmentReplicantLookup.getTotalReplicants(segment.getIdentifier()) == 0) ? 0 : 1;
      retVal.add(segment.getDataSource(), 1 - available);
    }

    return retVal;
  }

  CountingMap<String> getLoadPendingDatasources()
  {
    final CountingMap<String> retVal = new CountingMap<>();
    for (LoadQueuePeon peon : loadManagementPeons.values()) {
      for (DataSegment segment : peon.getSegmentsToLoad()) {
        retVal.add(segment.getDataSource(), 1);
      }
    }
    return retVal;
  }

  public Map<String, Double> getLoadStatus()
  {
    Map<String, Double> loadStatus = Maps.newHashMap();
    for (DruidDataSource dataSource : metadataSegmentManager.getInventory()) {
      final Set<DataSegment> segments = Sets.newHashSet(dataSource.getSegments());
      final int availableSegmentSize = segments.size();

      // remove loaded segments
      for (DruidServer druidServer : serverInventoryView.getInventory()) {
        final DruidDataSource loadedView = druidServer.getDataSource(dataSource.getName());
        if (loadedView != null) {
          segments.removeAll(loadedView.getSegments());
        }
      }
      final int unloadedSegmentSize = segments.size();
      loadStatus.put(
          dataSource.getName(),
          100 * ((double) (availableSegmentSize - unloadedSegmentSize) / (double) availableSegmentSize)
      );
    }

    return loadStatus;
  }

  public CoordinatorDynamicConfig getDynamicConfigs()
  {
    return configManager.watch(
        CoordinatorDynamicConfig.CONFIG_KEY,
        CoordinatorDynamicConfig.class,
        defaultConfig
    ).get();
  }

  public void disableSegment(String reason, DataSegment segment)
  {
    log.info("Disable Segment[%s] for [%s]", segment.getIdentifier(), reason);
    metadataSegmentManager.disableSegment(segment.getDataSource(), segment.getIdentifier());
  }

  public void enableDatasource(String ds)
  {
    metadataSegmentManager.enableDatasource(ds);
  }

  public String getCurrentLeader()
  {
    try {
      final LeaderLatch latch = leaderLatch.get();

      if (latch == null) {
        return null;
      }

      Participant participant = latch.getLeader();
      if (participant.isLeader()) {
        return participant.getId();
      }

      return null;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void moveSegment(
      ImmutableDruidServer fromServer,
      ImmutableDruidServer toServer,
      String segmentName,
      final LoadPeonCallback callback
  )
  {
    try {
      if (fromServer.getMetadata().equals(toServer.getMetadata())) {
        throw new IAE("Cannot move [%s] to and from the same server [%s]", segmentName, fromServer.getName());
      }

      final DataSegment segment = fromServer.getSegment(segmentName);
      if (segment == null) {
        throw new IAE("Unable to find segment [%s] on server [%s]", segmentName, fromServer.getName());
      }

      final LoadQueuePeon loadPeon = loadManagementPeons.get(toServer.getName());
      if (loadPeon == null) {
        throw new IAE("LoadQueuePeon hasn't been created yet for path [%s]", toServer.getName());
      }

      final LoadQueuePeon dropPeon = loadManagementPeons.get(fromServer.getName());
      if (dropPeon == null) {
        throw new IAE("LoadQueuePeon hasn't been created yet for path [%s]", fromServer.getName());
      }

      final ServerHolder toHolder = new ServerHolder(toServer, loadPeon);
      if (toHolder.getAvailableSize() < segment.getSize()) {
        throw new IAE(
            "Not enough capacity on server [%s] for segment [%s]. Required: %,d, available: %,d.",
            toServer.getName(),
            segment,
            segment.getSize(),
            toHolder.getAvailableSize()
        );
      }

      // served path check here was not valid (always null).. so removed
      // just regard removed-from-queue means it's served (little racy but would be faster than unload + unannounce)
      final String toLoadQueueSegPath = ZKPaths.makePath(loadPeon.getBasePath(), segmentName);

      loadPeon.loadSegment(
          segment,
          "balancing",
          new LoadPeonCallback()
          {
            @Override
            public void execute()
            {
              try {
                if (curator.checkExists().forPath(toLoadQueueSegPath) == null) {
                  dropPeon.dropSegment(segment, "balancing", callback);
                } else if (callback != null) {
                  callback.execute();
                }
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
            }
          }
      );
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception moving segment %s", segmentName).emit();
      if (callback != null) {
        callback.execute();
      }
    }
  }

  public Set<DataSegment> getOrderedAvailableDataSegments()
  {
    return makeOrdered(getAvailableDataSegments());
  }

  public Set<DataSegment> makeOrdered(Iterable<DataSegment> dataSegments)
  {
    Set<DataSegment> availableSegments = Sets.newTreeSet(SEGMENT_COMPARATOR);

    for (DataSegment dataSegment : dataSegments) {
      if (dataSegment.getSize() < 0) {
        log.makeAlert("No size on Segment")
           .addData("segment", dataSegment)
           .emit();
      }
      availableSegments.add(dataSegment);
    }

    return availableSegments;
  }

  public Iterable<DataSegment> getAvailableDataSegments()
  {
    return Iterables.concat(
        Iterables.transform(
            metadataSegmentManager.getInventory(),
            new Function<DruidDataSource, Iterable<DataSegment>>()
            {
              @Override
              public Iterable<DataSegment> apply(DruidDataSource input)
              {
                return input.getSegments();
              }
            }
        )
    );
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }
      started = true;

      createNewLeaderLatch();
      try {
        leaderLatch.get().start();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private LeaderLatch createNewLeaderLatch()
  {
    final LeaderLatch newLeaderLatch = new LeaderLatch(
        curator, ZKPaths.makePath(zkPaths.getCoordinatorPath(), COORDINATOR_OWNER_NODE), self.getHostAndPort()
    );

    newLeaderLatch.addListener(
        new LeaderLatchListener()
        {
          @Override
          public void isLeader()
          {
            DruidCoordinator.this.becomeLeader();
          }

          @Override
          public void notLeader()
          {
            DruidCoordinator.this.stopBeingLeader();
          }
        },
        Execs.singleThreaded("CoordinatorLeader-%s")
    );

    return leaderLatch.getAndSet(newLeaderLatch);
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      stopBeingLeader();

      try {
        leaderLatch.get().close();
      }
      catch (IOException e) {
        log.warn(e, "Unable to close leaderLatch, ignoring");
      }

      started = false;

      exec.shutdownNow();
      balancerExec.shutdownNow();
    }
  }

  private void becomeLeader()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      log.info("I am the leader of the coordinators, all must bow!");
      log.info("Starting coordination in [%s]", config.getCoordinatorStartDelay());
      try {
        leaderCounter++;
        leader = true;
        metadataSegmentManager.start();
        metadataRuleManager.start();
        serverInventoryView.start();
        serviceAnnouncer.announce(self);
        final int startingLeaderCounter = leaderCounter;

        final List<Pair<? extends CoordinatorRunnable, Duration>> coordinatorRunnables = Lists.newArrayList();
        coordinatorRunnables.add(
            Pair.of(
                new CoordinatorHistoricalManagerRunnable(startingLeaderCounter),
                config.getCoordinatorPeriod()
            )
        );
        if (indexingServiceClient != null) {
          coordinatorRunnables.add(
              Pair.of(
                  new CoordinatorIndexingServiceRunnable(
                      makeIndexingServiceHelpers(),
                      startingLeaderCounter
                  ),
                  config.getCoordinatorIndexingPeriod()
              )
          );
        }

        for (final Pair<? extends CoordinatorRunnable, Duration> coordinatorRunnable : coordinatorRunnables) {
          ScheduledExecutors.scheduleWithFixedDelay(
              exec,
              config.getCoordinatorStartDelay(),
              coordinatorRunnable.rhs,
              new Callable<ScheduledExecutors.Signal>()
              {
                private final CoordinatorRunnable theRunnable = coordinatorRunnable.lhs;

                @Override
                public ScheduledExecutors.Signal call()
                {
                  if (leader && startingLeaderCounter == leaderCounter) {
                    theRunnable.run();
                  }
                  if (leader && startingLeaderCounter == leaderCounter) { // (We might no longer be leader)
                    return ScheduledExecutors.Signal.REPEAT;
                  } else {
                    return ScheduledExecutors.Signal.STOP;
                  }
                }

                @Override
                public String toString()
                {
                  return theRunnable.toString();
                }
              }
          );
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to become leader")
           .emit();
        final LeaderLatch oldLatch = createNewLeaderLatch();
        CloseQuietly.close(oldLatch);
        try {
          leaderLatch.get().start();
        }
        catch (Exception e1) {
          // If an exception gets thrown out here, then the coordinator will zombie out 'cause it won't be looking for
          // the latch anymore.  I don't believe it's actually possible for an Exception to throw out here, but
          // Curator likes to have "throws Exception" on methods so it might happen...
          log.makeAlert(e1, "I am a zombie")
             .emit();
        }
      }
    }
  }

  // wish not to be called too frequently..
  public CoordinatorStats scheduleNow(
      final Set<DataSegment> segments,
      final boolean assertLoaded,
      final long waitTimeout
  )
      throws InterruptedException, ExecutionException, TimeoutException

  {
    if (!assertLoaded) {
      return scheduleNow(segments).get(waitTimeout, TimeUnit.MILLISECONDS);
    }
    final CountDownLatch latch = new CountDownLatch(segments.size());
    final ServerView.BaseSegmentCallback callback = new ServerView.BaseSegmentCallback()
    {
      @Override
      public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
      {
        latch.countDown();
        return ServerView.CallbackAction.CONTINUE;
      }
    };
    serverInventoryView.registerSegmentCallback(exec, callback);

    long start = System.currentTimeMillis();
    try {
      CoordinatorStats stats = scheduleNow(segments).get(waitTimeout, TimeUnit.MILLISECONDS);
      long remain = waitTimeout - (System.currentTimeMillis() - start);
      if (remain <= 0 || !latch.await(remain, TimeUnit.MILLISECONDS)) {
        throw new TimeoutException("At least " + latch.getCount() + " segments are not loaded yet");
      }
      return stats;
    }
    finally {
      serverInventoryView.removeSegmentCallback(callback);
    }
  }

  private Future<CoordinatorStats> scheduleNow(Set<DataSegment> segments)
  {
    return runNow(segments, new DruidCoordinatorRuleRunner(DruidCoordinator.this));
  }

  private Future<CoordinatorStats> balanceNow()
  {
    return runNow(Collections.<DataSegment>emptySet(), new DruidCoordinatorBalancer(DruidCoordinator.this));
  }

  private Future<CoordinatorStats> runNow(final Set<DataSegment> segments, final DruidCoordinatorHelper helper)
  {
    return exec.submit(
          new Callable<CoordinatorStats>()
          {
            @Override
            public CoordinatorStats call()
            {
              DruidCoordinatorRuntimeParams params = buildParam(
                  DruidCoordinatorRuntimeParams.newBuilder().withAvailableSegments(segments)
              );
              if (leader) {
                final List<Rule> loader = Arrays.<Rule>asList(ForeverLoadRule.of(1));
                params = params.buildFromExisting()
                               .withDatabaseRuleManager(
                                   new MetadataRuleManager.Abstract()
                                   {
                                     @Override
                                     public List<Rule> getRulesWithDefault(String dataSource) { return loader;}
                                   }
                               ).build();
                helper.run(params);
              }
              return params.getCoordinatorStats();
            }

            @Override
            public String toString()
            {
              return "scheduleNow";
            }
          }
      );
  }

  private void stopBeingLeader()
  {
    synchronized (lock) {
      try {
        leaderCounter++;

        log.info("I am no longer the leader...");

        for (String server : loadManagementPeons.keySet()) {
          LoadQueuePeon peon = loadManagementPeons.remove(server);
          peon.stop();
        }
        loadManagementPeons.clear();

        serviceAnnouncer.unannounce(self);
        serverInventoryView.stop();
        metadataRuleManager.stop();
        metadataSegmentManager.stop();
        leader = false;
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to stopBeingLeader").emit();
      }
    }
  }

  private List<DruidCoordinatorHelper> makeIndexingServiceHelpers()
  {
    List<DruidCoordinatorHelper> helpers = Lists.newArrayList();
    helpers.add(new DruidCoordinatorSegmentInfoLoader(DruidCoordinator.this));
    helpers.addAll(indexingServiceHelpers);

    log.info("Done making indexing service helpers [%s]", helpers);
    return ImmutableList.copyOf(helpers);
  }

  // keep latest only ?
  // seemed possible to disable the segment
  private final Map<DataSegment, Set<StringKeyed<Long>>> reports = Maps.newHashMap();

  public void reportSegmentFileNotFound(String server, Set<DataSegment> segments)
  {
    final long current = System.currentTimeMillis();
    final long threshold = current - (config.getCoordinatorPeriod().getMillis() << 2);
    synchronized (reports) {
      for (DataSegment segment : segments) {
        Set<StringKeyed<Long>> report = reports.get(segment);
        if (report == null) {
          reports.put(segment, report = Sets.newHashSet());
        }
        expire(report, threshold);
        report.add(StringKeyed.of(server, current));
      }
    }
  }

  public Pair<Long, List<String>> getRecentlyFailedServers(DataSegment segment)
  {
    synchronized (reports) {
      Set<StringKeyed<Long>> report = reports.get(segment);
      if (report != null) {
        expire(report, System.currentTimeMillis() - (config.getCoordinatorPeriod().getMillis() << 2));
        if (report.isEmpty()) {
          reports.remove(segment);
          return null;
        }
        Long max = null;
        List<String> servers = Lists.newArrayListWithCapacity(report.size());
        for (StringKeyed<Long> server : report) {
          servers.add(server.key);
          if (max == null || max < server.value) {
            max = server.value;
          }
        }
        return Pair.of(max, servers);
      }
      return null;
    }
  }

  private void expire(Set<StringKeyed<Long>> report, long threshold)
  {
    final Iterator<StringKeyed<Long>> iterator = report.iterator();
    while (iterator.hasNext()) {
      if (iterator.next().value < threshold) {
        iterator.remove();
      }
    }
  }

  public abstract class CoordinatorRunnable implements Runnable
  {
    private final long startTime = System.currentTimeMillis();
    private final List<DruidCoordinatorHelper> helpers;
    private final int startingLeaderCounter;
    private final AtomicInteger counter = new AtomicInteger();
    private final int lazyTick;

    protected CoordinatorRunnable(List<DruidCoordinatorHelper> helpers, int startingLeaderCounter)
    {
      this.helpers = helpers;
      this.startingLeaderCounter = startingLeaderCounter;
      this.lazyTick = config.getCoordinatorLazyTicks();
    }

    @Override
    public void run()
    {
      try {
        synchronized (lock) {
          final LeaderLatch latch = leaderLatch.get();
          if (latch == null || !latch.hasLeadership()) {
            log.info("LEGGO MY EGGO. [%s] is leader.", latch == null ? null : latch.getLeader().getId());
            stopBeingLeader();
            return;
          }
        }

        List<Boolean> allStarted = Arrays.asList(
            metadataSegmentManager.isStarted(),
            serverInventoryView.isStarted()
        );
        for (Boolean aBoolean : allStarted) {
          if (!aBoolean) {
            log.error("InventoryManagers not started[%s]", allStarted);
            stopBeingLeader();
            return;
          }
        }

        // Do coordinator stuff.
        DruidCoordinatorRuntimeParams params =
            DruidCoordinatorRuntimeParams.newBuilder()
                                         .withMajorTick(counter.getAndIncrement() % lazyTick == 0)
                                         .withStartTime(startTime)
                                         .withDatasources(metadataSegmentManager.getInventory())
                                         .withDynamicConfigs(getDynamicConfigs())
                                         .withEmitter(emitter)
                                         .build();
        for (DruidCoordinatorHelper helper : helpers) {
          // Don't read state and run state in the same helper otherwise racy conditions may exist
          if (leader && startingLeaderCounter == leaderCounter) {
            params = helper.run(params);
          }
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Caught exception, ignoring so that schedule keeps going.").emit();
      }
    }

    @Override
    public String toString()
    {
      return getClass().getSimpleName();
    }
  }

  private class CoordinatorHistoricalManagerRunnable extends CoordinatorRunnable
  {
    public CoordinatorHistoricalManagerRunnable(final int startingLeaderCounter)
    {
      super(
          ImmutableList.of(
              new DruidCoordinatorSegmentInfoLoader(DruidCoordinator.this),
              new DruidCoordinatorHelper()
              {
                @Override
                public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
                {
                  return prevParam = buildParam(params.buildFromExisting());
                }
              },
              new DruidCoordinatorRuleRunner(DruidCoordinator.this),
              new DruidCoordinatorCleanupUnneeded(),
              new DruidCoordinatorCleanupOvershadowed(DruidCoordinator.this),
              new DruidCoordinatorBalancer(DruidCoordinator.this),
              new DruidCoordinatorLogger()
          ),
          startingLeaderCounter
      );
    }
  }

  private DruidCoordinatorRuntimeParams buildParam(DruidCoordinatorRuntimeParams.Builder builder)
  {
    // Display info about all historical servers
    List<ImmutableDruidServer> servers = Lists.newArrayList(
        Iterables.transform(
            Iterables.filter(
                serverInventoryView.getInventory(),
                new Predicate<DruidServer>()
                {
                  @Override
                  public boolean apply(DruidServer input)
                  {
                    return input.isAssignable();
                  }
                }
            ),
            DruidServer.IMMUTABLE
        )
    );

    if (log.isDebugEnabled()) {
      log.debug("Servers");
      for (ImmutableDruidServer druidServer : servers) {
        log.debug("  %s", druidServer);
        log.debug("    -- DataSources");
        for (ImmutableDruidDataSource druidDataSource : druidServer.getDataSources()) {
          log.debug(
              "    %s : properties=%s, %,d segments",
              druidDataSource.getName(),
              druidDataSource.getProperties(),
              druidDataSource.size()
          );
        }
      }
    }

    // Find all historical servers, group them by subType and sort by ascending usage
    final DruidCluster cluster = new DruidCluster();
    for (ImmutableDruidServer server : servers) {
      if (!loadManagementPeons.containsKey(server.getName())) {
        LoadQueuePeon loadQueuePeon = taskMaster.giveMePeon(zkPaths.getLoadQueuePath(), server.getName());
        log.info("Creating LoadQueuePeon for server[%s] at path[%s]", server.getName(), loadQueuePeon.getBasePath());

        loadManagementPeons.put(server.getName(), loadQueuePeon);
      }

      cluster.add(new ServerHolder(server, loadManagementPeons.get(server.getName())));
    }

    // Stop peons for servers that aren't there anymore.
    final Set<String> disappeared = Sets.newHashSet(loadManagementPeons.keySet());
    for (ImmutableDruidServer server : servers) {
      disappeared.remove(server.getName());
    }
    for (String name : disappeared) {
      log.info("Removing listener for server[%s] which is no longer there.", name);
      LoadQueuePeon peon = loadManagementPeons.remove(name);
      peon.stop();
    }

    return builder.withDruidCluster(cluster)
                  .withDatabaseRuleManager(metadataRuleManager)
                  .withLoadManagementPeons(loadManagementPeons)
                  .withSegmentReplicantLookup(SegmentReplicantLookup.make(cluster))
                  .withBalancerReferenceTimestamp(DateTime.now())
                  .withBalancerStrategy(factory.createBalancerStrategy(balancerExec))
                  .withReplicationManager(replicatorThrottler)
                  .build();
  }

  private class CoordinatorIndexingServiceRunnable extends CoordinatorRunnable
  {
    public CoordinatorIndexingServiceRunnable(List<DruidCoordinatorHelper> helpers, final int startingLeaderCounter)
    {
      super(helpers, startingLeaderCounter);
    }
  }
}

