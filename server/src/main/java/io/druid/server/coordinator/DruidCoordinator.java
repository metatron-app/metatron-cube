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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.ServerInventoryView;
import io.druid.client.ServerView;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.collections.CountingMap;
import io.druid.common.config.JacksonConfigManager;
import io.druid.common.guava.GuavaUtils;
import io.druid.concurrent.Execs;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.data.KeyedData.StringKeyed;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.CoordinatorIndexingServiceHelper;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import java.util.function.Predicate;

/**
 */
@ManageLifecycle
public class DruidCoordinator
{
  public static final String COORDINATOR_OWNER_NODE = "_COORDINATOR";

  private static final EmittingLogger log = new EmittingLogger(DruidCoordinator.class);
  private final Object lock = new Object();
  private final DruidCoordinatorConfig config;
  private final ZkPathsConfig zkPaths;
  private final JacksonConfigManager configManager;
  private final MetadataSegmentManager metadataSegmentManager;
  private final ServerInventoryView serverInventoryView;
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

    this.exec = scheduledExecutorFactory.create(
        1, String.format("Coordinator-Exec(%s)", config.getCoordinatorPeriod())
    );

    this.leaderLatch = new AtomicReference<>(null);
    this.loadManagementPeons = loadQueuePeonMap;
    this.defaultConfig = defaultConfig == null ? new CoordinatorDynamicConfig() : defaultConfig;
    this.factory = factory;

    final CoordinatorDynamicConfig dynamicConfigs = getDynamicConfigs();
    this.balancerExec = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(dynamicConfigs.getBalancerComputeThreads())
    );
    if (serverInventoryView.toString().contains("EasyMock")) {
      return;   // damn easy mock
    }
    serverInventoryView.registerServerCallback(
        exec,
        new ServerView.ServerCallback()
        {
          @Override
          public ServerView.CallbackAction serverAdded(DruidServer server)
          {
            if (leader) {
              balanceNow();
            }
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction serverRemoved(DruidServer server)
          {
            if (leader) {
              serverDown(server);
            }
            return ServerView.CallbackAction.CONTINUE;
          }

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

  public boolean isReady()
  {
    return leader && metadataSegmentManager.lastUpdatedTime() != null;
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
    for (DataSegment segment : getAvailableDataSegments().get()) {
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

    for (DataSegment segment : getAvailableDataSegments().get()) {
      int available = segmentReplicantLookup.getTotalReplicants(segment.getIdentifier()) == 0 ? 0 : 1;
      retVal.add(segment.getDataSource(), 1 - available);
    }

    return retVal;
  }

  CountingMap<String> getLoadPendingDatasources()
  {
    final CountingMap<String> retVal = new CountingMap<>();
    for (LoadQueuePeon peon : loadManagementPeons.values()) {
      peon.getSegmentsToLoad(segment -> retVal.add(segment.getDataSource(), 1));
    }
    return retVal;
  }

  public Map<String, Double> getLoadStatus()
  {
    final Map<String, Double> loadStatus = Maps.newHashMap();
    for (DruidDataSource dataSource : metadataSegmentManager.getInventory()) {
      final Set<String> segmentIds = Sets.newHashSet(dataSource.getCopyOfSegmentIds());
      final int availableSegmentSize = segmentIds.size();
      if (availableSegmentSize == 0) {
        continue;
      }
      // remove loaded segments
      for (DruidServer druidServer : serverInventoryView.getInventory()) {
        final DruidDataSource loadedView = druidServer.getDataSource(dataSource.getName());
        if (loadedView != null) {
          for (String segmentId : loadedView.getCopyOfSegmentIdsAsList()) {
            segmentIds.remove(segmentId);
          }
        }
      }
      final int unloadedSegmentSize = segmentIds.size();
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

  public boolean disableSegment(String reason, DataSegment segment)
  {
    log.info("Disable Segment[%s] for [%s]", segment.getIdentifier(), reason);
    return metadataSegmentManager.disableSegment(segment.getDataSource(), segment.getIdentifier());
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

  // conditions like availablity, loading state, etc. should be checked before
  public boolean moveSegment(
      final DataSegment segment,
      final ServerHolder fromServer,
      final ServerHolder toServer,
      final LoadPeonCallback callback,
      final Predicate<DataSegment> validity   // load validity
  )
  {
    final String segmentId = segment.getIdentifier();
    try {
      if (Objects.equals(fromServer.getName(), toServer.getName())) {
        throw new IAE("Cannot move [%s] to and from the same server [%s]", segmentId, fromServer.getName());
      }
      final LoadQueuePeon loadPeon = toServer.getPeon();
      final LoadQueuePeon dropPeon = fromServer.getPeon();
      // served path check here was not valid (always null).. so removed
      // just regard removed-from-queue means it's served (little racy but would be faster than unload + unannounce)
      final String toLoadQueueSegPath = ZKPaths.makePath(loadPeon.getBasePath(), segmentId);

      loadPeon.loadSegment(
          segment,
          String.format("balancing from %s", fromServer.getName()),
          new LoadPeonCallback()
          {
            @Override
            public void execute(boolean canceled)
            {
              if (canceled) {
                return;   // nothing to do
              }
              try {
                if (curator.checkExists().forPath(toLoadQueueSegPath) == null) {
                  dropPeon.dropSegment(segment, String.format("balanced to %s", toServer.getName()), callback, null);
                } else if (callback != null) {
                  callback.execute(canceled);
                }
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
            }
          },
          validity
      );
      return true;
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception moving segment %s", segmentId).emit();
      if (callback != null) {
        callback.execute(true);
      }
    }
    return false;
  }

  public Supplier<Iterable<DataSegment>> getAvailableDataSegments()
  {
    return () -> Iterables.concat(Iterables.transform(
        metadataSegmentManager.getInventory(), DruidDataSource::getCopyOfSegments
    ));
  }

  public boolean isAvailable(DataSegment segment)
  {
    return metadataSegmentManager.isAvailable(segment);
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
        serviceAnnouncer.announce(self);
        final int startingLeaderCounter = leaderCounter;

        final List<Pair<? extends CoordinatorRunnable, Duration>> coordinatorRunnables = Lists.newArrayList();
        coordinatorRunnables.add(
            Pair.of(
                new CoordinatorHistoricalManagerRunnable(startingLeaderCounter),
                config.getCoordinatorPeriod()
            )
        );
        if (indexingServiceClient != null && !GuavaUtils.isNullOrEmpty(indexingServiceHelpers)) {
          coordinatorRunnables.add(
              Pair.of(
                  new CoordinatorIndexingServiceRunnable(startingLeaderCounter),
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

  private Future<CoordinatorStats> serverDown(DruidServer server)
  {
    // stop immediately if the server revives
    return scheduleNow(ImmutableSet.copyOf(server.getSegments().values()), MetadataRuleManager.of(
        ForeverLoadRule.of(1, segment -> serverInventoryView.getInventoryValue(server.getName()) == null)
    ));
  }

  private Future<CoordinatorStats> scheduleNow(Set<DataSegment> segments)
  {
    return scheduleNow(segments, MetadataRuleManager.of(ForeverLoadRule.of(1, null)));
  }

  private Future<CoordinatorStats> scheduleNow(Set<DataSegment> segments, MetadataRuleManager manager)
  {
    return runNow(segments, manager, new DruidCoordinatorRuleRunner(this), false, "scheduleNow");
  }

  private Future<CoordinatorStats> balanceNow()
  {
    MetadataRuleManager manager = MetadataRuleManager.of(ForeverLoadRule.of(1, null));
    return runNow(ImmutableSet.<DataSegment>of(), manager, new DruidCoordinatorBalancer(this), true, "balanceNow");
  }

  private Future<CoordinatorStats> runNow(
      final Set<DataSegment> segments,
      final MetadataRuleManager ruleManager,
      final DruidCoordinatorHelper helper,
      final boolean majorTick,
      final String action
  )
  {
    return exec.submit(
        new Callable<CoordinatorStats>()
        {
          @Override
          public CoordinatorStats call()
          {
            DruidCoordinatorRuntimeParams params = buildParam(
                DruidCoordinatorRuntimeParams.newBuilder()
                                             .withMajorTick(majorTick)
                                             .withStartTime(System.currentTimeMillis())
                                             .withPollingInterval(config.getCoordinatorPeriod().getMillis())
                                             .withDatabaseRuleManager(ruleManager)
                                             .withAvailableSegments(Suppliers.ofInstance(segments))
            );
            if (leader) {
              for (DataSegment segment : segments) {
                metadataSegmentManager.registerToView(segment);   // for isAvailable() to return true
              }
              helper.run(params);
            }
            return params.getCoordinatorStats();
          }

          @Override
          public String toString()
          {
            return action;
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
        metadataRuleManager.stop();
        metadataSegmentManager.stop();
        leader = false;
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to stopBeingLeader").emit();
      }
    }
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

  public Pair<Long, Set<String>> getRecentlyFailedServers(DataSegment segment)
  {
    synchronized (reports) {
      final Set<StringKeyed<Long>> report = reports.get(segment);
      if (report != null) {
        expire(report, System.currentTimeMillis() - (config.getCoordinatorPeriod().getMillis() << 2));
        if (report.isEmpty()) {
          reports.remove(segment);
          return null;
        }
        Long max = null;
        final Set<String> servers = Sets.newHashSet();
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

        if (!metadataSegmentManager.isStarted()) {
          log.error("MetadataSegmentManager is not started");
          stopBeingLeader();
        }
        if (!serverInventoryView.isStarted()) {
          log.error("InventoryManager is not started");
          stopBeingLeader();
        }

        // prevent cleanup before polling is ended
        final boolean ready = metadataSegmentManager.lastUpdatedTime() != null;

        // Do coordinator stuff.
        DruidCoordinatorRuntimeParams params =
            DruidCoordinatorRuntimeParams.newBuilder()
                                         .withMajorTick(ready && counter.getAndIncrement() % lazyTick == 0)
                                         .withStartTime(startTime)
                                         .withPollingInterval(config.getCoordinatorPeriod().getMillis())
                                         .withDatasources(metadataSegmentManager.getInventory())
                                         .withAvailableSegments(getAvailableDataSegments())
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
              new DruidCoordinatorHelper()
              {
                @Override
                public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
                {
                  return prevParam = buildParam(params.buildFromExisting());
                }
              },
              new DruidCoordinatorRuleRunner(DruidCoordinator.this),
              new DruidCoordinatorCleanupUnneeded(DruidCoordinator.this),
              new DruidCoordinatorCleanupOvershadowed(DruidCoordinator.this),
              new DruidCoordinatorBalancer(DruidCoordinator.this),
              new DruidCoordinatorLogger(DruidCoordinator.this)
          ),
          startingLeaderCounter
      );
    }
  }

  private DruidCoordinatorRuntimeParams buildParam(DruidCoordinatorRuntimeParams.Builder builder)
  {
    // Display info about all historical servers
    final List<ImmutableDruidServer> servers = Lists.newArrayList(
        Iterables.transform(
            Iterables.filter(serverInventoryView.getInventory(), DruidServer::isAssignable),
            DruidServer::toImmutableDruidServer
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
    final Ordering<Comparable> ordering = Ordering.natural().reverse();
    final Map<String, MinMaxPriorityQueue<ServerHolder>> tierMap = Maps.newLinkedHashMap();
    for (ImmutableDruidServer server : servers) {
      LoadQueuePeon loadQueuePeon = loadManagementPeons.get(server.getName());
      if (loadQueuePeon != null) {
        loadQueuePeon.tick(defaultConfig.getReplicantLifetime());
      } else {
        loadQueuePeon = taskMaster.giveMePeon(zkPaths.getLoadQueuePath(), server.getName());
        log.info("Creating LoadQueuePeon for server[%s] at path[%s]", server.getName(), loadQueuePeon.getBasePath());
        loadManagementPeons.put(server.getName(), loadQueuePeon);
      }
      tierMap.computeIfAbsent(server.getTier(), k -> MinMaxPriorityQueue.orderedBy(ordering).create())
             .add(new ServerHolder(server, loadQueuePeon));
    }

    // Stop peons for servers that aren't there anymore.
    final Set<String> disappeared = Sets.newHashSet(loadManagementPeons.keySet());
    for (ImmutableDruidServer server : servers) {
      disappeared.remove(server.getName());
    }
    for (String name : disappeared) {
      LoadQueuePeon peon = loadManagementPeons.remove(name);
      if (peon != null) {
        log.info("Removing listener for server[%s] which is no longer there.", name);
        peon.stop();
      }
    }

    final DruidCluster cluster = new DruidCluster(tierMap);

    return builder.withDruidCluster(cluster)
                  .withDatabaseRuleManager(metadataRuleManager)
                  .withLoadManagementPeons(loadManagementPeons)
                  .withSegmentReplicantLookup(SegmentReplicantLookup.make(cluster))
                  .withBalancerStrategy(factory.createBalancerStrategy(balancerExec))
                  .build();
  }

  private class CoordinatorIndexingServiceRunnable extends CoordinatorRunnable
  {
    public CoordinatorIndexingServiceRunnable(final int startingLeaderCounter)
    {
      super(ImmutableList.copyOf(indexingServiceHelpers), startingLeaderCounter);
    }
  }
}

