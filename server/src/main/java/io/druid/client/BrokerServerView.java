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

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.http.client.HttpClient;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.client.selector.TierSelectorStrategy;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.Client;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.CPUTimeMetricQueryRunner;
import io.druid.query.DataSource;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryWatcher;
import io.druid.query.ReferenceCountingSegmentQueryRunner;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.QueryableIndex;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class BrokerServerView implements TimelineServerView
{
  private static final Logger log = new Logger(BrokerServerView.class);

  private final Object lock = new Object();

  private final DruidServer node;
  private final QueryRunnerFactoryConglomerate conglomerate;

  private final ConcurrentMap<String, QueryableDruidServer> clients;
  private final Map<String, ServerSelector> selectors;
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines;
  private final ConcurrentMap<TimelineCallback, Executor> timelineCallbacks = new ConcurrentHashMap<>();

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final FilteredServerInventoryView baseView;
  private final TierSelectorStrategy tierSelectorStrategy;
  private final ServiceEmitter emitter;
  private final ExecutorService backgroundExecutorService;
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> segmentFilter;

  private volatile boolean initialized = false;

  @Inject
  public BrokerServerView(
      @Self DruidNode node,
      QueryRunnerFactoryConglomerate conglomerate,
      QueryToolChestWarehouse warehouse,
      QueryWatcher queryWatcher,
      @Smile ObjectMapper smileMapper,
      @Client HttpClient httpClient,
      FilteredServerInventoryView baseView,
      TierSelectorStrategy tierSelectorStrategy,
      ServiceEmitter emitter,
      final BrokerSegmentWatcherConfig segmentWatcherConfig,
      @Processing ExecutorService backgroundExecutorService
  )
  {
    this.node = node == null ? null : new DruidServer(node, new DruidServerConfig(), "broker");
    this.conglomerate = conglomerate;
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.baseView = baseView;
    this.tierSelectorStrategy = tierSelectorStrategy;
    this.emitter = emitter;
    this.backgroundExecutorService = backgroundExecutorService;
    this.clients = Maps.newConcurrentMap();
    this.selectors = Maps.newHashMap();
    this.timelines = Maps.newHashMap();

    this.segmentFilter = new Predicate<Pair<DruidServerMetadata, DataSegment>>()
    {
      @Override
      public boolean apply(
          Pair<DruidServerMetadata, DataSegment> input
      )
      {
        if (segmentWatcherConfig.getWatchedTiers() != null
            && !segmentWatcherConfig.getWatchedTiers().contains(input.lhs.getTier())) {
          return false;
        }

        if (segmentWatcherConfig.getWatchedDataSources() != null
            && !segmentWatcherConfig.getWatchedDataSources().contains(input.rhs.getDataSource())) {
          return false;
        }

        return true;
      }
    };
    ExecutorService exec = Execs.singleThreaded("BrokerServerView-%s");
    baseView.registerSegmentCallback(
        exec,
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            serverAddedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DruidServerMetadata server, DataSegment segment)
          {
            serverRemovedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public CallbackAction segmentViewInitialized()
          {
            initialized = true;
            executeCallbacks(
                new Function<TimelineCallback, CallbackAction>()
                {
                  @Override
                  public CallbackAction apply(TimelineCallback timelineCallback)
                  {
                    return timelineCallback.timelineInitialized();
                  }
                }
            );
            return ServerView.CallbackAction.CONTINUE;
          }
        },
        segmentFilter
    );

    baseView.registerServerCallback(
        exec,
        new ServerView.ServerCallback()
        {
          @Override
          public ServerView.CallbackAction serverRemoved(DruidServer server)
          {
            removeServer(server);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  public boolean isInitialized()
  {
    return initialized;
  }

  public void clear()
  {
    synchronized (lock) {
      final Iterator<String> clientsIter = clients.keySet().iterator();
      while (clientsIter.hasNext()) {
        clientsIter.remove();
      }

      timelines.clear();

      final Iterator<ServerSelector> selectorsIter = selectors.values().iterator();
      while (selectorsIter.hasNext()) {
        final ServerSelector selector = selectorsIter.next();
        selectorsIter.remove();
        while (!selector.isEmpty()) {
          final QueryableDruidServer pick = selector.pick();
          selector.removeServer(pick);
        }
      }
    }
  }

  private DirectDruidClient makeDirectClient(DruidServer server)
  {
    return new DirectDruidClient(
        warehouse,
        queryWatcher,
        smileMapper,
        httpClient,
        server.getHost(),
        emitter,
        backgroundExecutorService
    );
  }

  private QueryableDruidServer removeServer(DruidServer server)
  {
    for (DataSegment segment : server.getSegments().values()) {
      serverRemovedSegment(server.getMetadata(), segment);
    }
    return clients.remove(server.getName());
  }

  private void serverAddedSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    log.debug("Adding segment[%s] for server[%s]", segment, server);
    synchronized (lock) {
      addSegment(baseView.getInventoryValue(server.getName()), segment);
    }
  }

  private QueryableDruidServer addSegment(final DruidServer server, final DataSegment segment)
  {
    String segmentId = segment.getIdentifier();
    ServerSelector selector = selectors.get(segmentId);
    if (selector == null) {
      selector = new ServerSelector(segment, tierSelectorStrategy);

      VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
      if (timeline == null) {
        timeline = new VersionedIntervalTimeline<>(Ordering.natural());
        timelines.put(segment.getDataSource(), timeline);
      }

      timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
      selectors.put(segmentId, selector);
    }

    QueryableDruidServer queryableDruidServer = clients.get(server.getName());
    if (queryableDruidServer == null) {
      DirectDruidClient client = server.equals(node) ? null : makeDirectClient(server);
      QueryableDruidServer retVal = new QueryableDruidServer(server, client);
      QueryableDruidServer exists = clients.put(server.getName(), retVal);
      if (exists != null) {
        log.warn("QueryRunner for server[%s] already existed!? Well it's getting replaced", server);
      }
      queryableDruidServer = retVal;
    }
    selector.addServerAndUpdateSegment(queryableDruidServer, segment);

    executeCallbacks(
        new Function<TimelineCallback, CallbackAction>()
        {
          @Override
          public CallbackAction apply(TimelineCallback input)
          {
            return input.segmentAdded(server.getMetadata(), segment);
          }
        }
    );
    return queryableDruidServer;
  }

  public void addedLocalSegment(DataSegment segment, QueryableIndex index, Map<String, Object> metaData)
  {
    log.debug("Adding local segment[%s]", segment);
    synchronized (lock) {
      addSegment(node, segment).addIndex(segment, index, metaData);
    }
  }

  public List<String> getLocalDataSources()
  {
    QueryableDruidServer localServer = clients.get(node.getName());
    if (localServer == null) {
      return ImmutableList.of();
    }
    return localServer.getLocalDataSources();
  }

  public Interval getLocalDataSourceCoverage(String dataSource)
  {
    QueryableDruidServer localServer = clients.get(node.getName());
    return localServer == null ? null : localServer.getLocalDataSourceCoverage(dataSource);
  }

  public Map<String, Object> getLocalDataSourceMeta(Iterable<String> dataSources, String queryId)
  {
    QueryableDruidServer localServer = clients.get(node.getName());
    return localServer == null ? null : localServer.getLocalDataSourceMetaData(dataSources, queryId);
  }

  public boolean dropLocalDataSource(String dataSource)
  {
    log.debug("Dropping local dataSource[%s]", dataSource);
    synchronized (lock) {
      QueryableDruidServer localServer = clients.get(node.getName());
      if (localServer == null) {
        return false;
      }
      VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline =
          localServer.getLocalTimelineView().remove(dataSource);
      if (timeline == null) {
        return false;
      }
      for (PartitionChunk<ReferenceCountingSegment> chunk : timeline.clear()) {
        try (ReferenceCountingSegment segment = chunk.getObject()) {
          serverRemovedSegment(node.getMetadata(), segment.dataSegment);
        }
        catch (IOException e) {
          log.info(e, "Failed to close local segment %s", chunk.getObject().dataSegment);
        }
      }
      return true;
    }
  }

  private void serverRemovedSegment(final DruidServerMetadata server, final DataSegment segment)
  {

    String segmentId = segment.getIdentifier();
    final ServerSelector selector;

    synchronized (lock) {
      log.debug("Removing segment[%s] from server[%s].", segmentId, server);

      selector = selectors.get(segmentId);
      if (selector == null) {
        log.warn("Told to remove non-existant segment[%s]", segmentId);
        return;
      }

      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (!selector.removeServer(queryableDruidServer)) {
        log.warn(
            "Asked to disassociate non-existant association between server[%s] and segment[%s]",
            server,
            segmentId
        );
      }

      if (selector.isEmpty()) {
        VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
        selectors.remove(segmentId);

        final PartitionChunk<ServerSelector> removedPartition = timeline.remove(
            segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector)
        );

        if (removedPartition == null) {
          log.warn(
              "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
              segment.getInterval(),
              segment.getVersion()
          );
        } else {
          executeCallbacks(
              new Function<TimelineCallback, CallbackAction>()
              {
                @Override
                public CallbackAction apply(TimelineCallback callback)
                {
                  return callback.segmentRemoved(server, segment);
                }
              }
          );
        }
      }
    }
  }


  @Override
  public VersionedIntervalTimeline<String, ServerSelector> getTimeline(DataSource dataSource)
  {
    String table = Iterables.getOnlyElement(dataSource.getNames());
    synchronized (lock) {
      return timelines.get(table);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
  {
    timelineCallbacks.put(callback, exec);
  }

  @Override
  public List<QueryableDruidServer> getServers()
  {
    return ImmutableList.copyOf(clients.values());
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(DruidServer server)
  {
    final QueryableDruidServer queryableDruidServer;
    synchronized (lock) {
      queryableDruidServer = clients.get(server.getName());
      if (queryableDruidServer == null) {
        log.error("WTF?! No QueryableDruidServer found for %s", server.getName());
        return new NoopQueryRunner<T>();
      }
    }
    DirectDruidClient client = queryableDruidServer.getClient();
    if (client != null) {
      return client;
    }
    // query from local segments
    Preconditions.checkArgument(node.equals(queryableDruidServer.getServer()));
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return toRunner(query, queryableDruidServer.getLocalTimelineView()).run(query, responseContext);
      }
    };
  }

  private <T> QueryRunner<T> toRunner(
      final Query<T> query,
      final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> indexMap
  )
  {
    // called from CachingClusteredClient.. it's always MultipleSpecificSegmentSpec
    final MultipleSpecificSegmentSpec segmentSpec = (MultipleSpecificSegmentSpec) query.getQuerySegmentSpec();

    final List<SegmentDescriptor> specs = segmentSpec.getDescriptors();
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.warn("Unknown query type, [%s]", query.getClass());
      return new NoopQueryRunner<T>();
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    final String dataSourceName = Iterables.getOnlyElement(query.getDataSource().getNames());

    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = indexMap.get(dataSourceName);
    if (timeline == null) {
      return new NoopQueryRunner<T>();
    }

    final Function<Query<T>, ServiceMetricEvent.Builder> builderFn = getBuilderFn(toolChest);
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    FunctionalIterable<QueryRunner<T>> queryRunners = FunctionalIterable
        .create(specs)
        .transformCat(
            new Function<SegmentDescriptor, Iterable<QueryRunner<T>>>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Iterable<QueryRunner<T>> apply(SegmentDescriptor input)
              {
                final PartitionHolder<ReferenceCountingSegment> entry = timeline.findEntry(
                    input.getInterval(), input.getVersion()
                );

                if (entry == null) {
                  return Arrays.<QueryRunner<T>>asList(new ReportTimelineMissingSegmentQueryRunner<T>(input));
                }

                final PartitionChunk<ReferenceCountingSegment> chunk = entry.getChunk(input.getPartitionNumber());
                if (chunk == null) {
                  return Arrays.<QueryRunner<T>>asList(new ReportTimelineMissingSegmentQueryRunner<T>(input));
                }

                final ReferenceCountingSegment adapter = chunk.getObject();
                return Arrays.asList(
                    buildAndDecorateQueryRunner(factory, toolChest, adapter, input, builderFn, cpuTimeAccumulator)
                );
              }
            }
        );

    ExecutorService executor = Execs.singleThreaded("BrokerLocalProcessor-%s");
    return CPUTimeMetricQueryRunner.safeBuild(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(executor, queryRunners, null)),
            toolChest
        ),
        builderFn,
        emitter,
        cpuTimeAccumulator,
        true
    );
  }

  // copied from server manager, except cache populator
  private <T> QueryRunner<T> buildAndDecorateQueryRunner(
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final ReferenceCountingSegment adapter,
      final SegmentDescriptor segmentDescriptor,
      final Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      final AtomicLong cpuTimeAccumulator
  )
  {
    SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(segmentDescriptor);
    return CPUTimeMetricQueryRunner.safeBuild(
        new SpecificSegmentQueryRunner<T>(
            new MetricsEmittingQueryRunner<T>(
                emitter,
                builderFn,
                new BySegmentQueryRunner<T>(
                    adapter.getIdentifier(),
                    adapter.getDataInterval().getStart(),
                        new MetricsEmittingQueryRunner<T>(
                            emitter,
                            new Function<Query<T>, ServiceMetricEvent.Builder>()
                            {
                              @Override
                              public ServiceMetricEvent.Builder apply(@Nullable final Query<T> input)
                              {
                                return toolChest.makeMetricBuilder(input);
                              }
                            },
                            new ReferenceCountingSegmentQueryRunner<T>(null, factory, adapter, segmentDescriptor, null),
                            "query/segment/time",
                            ImmutableMap.of("segment", adapter.getIdentifier())
                        )
                ),
                "query/segmentAndCache/time",
                ImmutableMap.of("segment", adapter.getIdentifier())
            ).withWaitMeasuredFromNow(),
            segmentSpec
        ),
        builderFn,
        emitter,
        cpuTimeAccumulator,
        false
    );
  }

  private static <T> Function<Query<T>, ServiceMetricEvent.Builder> getBuilderFn(final QueryToolChest<T, Query<T>> toolChest)
  {
    return new Function<Query<T>, ServiceMetricEvent.Builder>()
    {
      @Override
      public ServiceMetricEvent.Builder apply(Query<T> input)
      {
        return toolChest.makeMetricBuilder(input);
      }
    };
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback)
  {
    baseView.registerServerCallback(exec, callback);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    baseView.registerSegmentCallback(exec, callback, segmentFilter);
  }

  private void executeCallbacks(final Function<TimelineCallback, CallbackAction> function)
  {
    for (final Map.Entry<TimelineCallback, Executor> entry : timelineCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == function.apply(entry.getKey())) {
                timelineCallbacks.remove(entry.getKey());
              }
            }
          }
      );
    }
  }
}
