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

package io.druid.client;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.EscalatedClient;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.jackson.JodaStuff;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.StatusResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.CPUTimeMetricBuilder;
import io.druid.query.DataSources;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.QueryRunners;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryWatcher;
import io.druid.query.ReferenceCountingSegmentQueryRunner;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.RowResolver;
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.DenseSegmentsSpec;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.ReferenceCountingSegment.LocalSegment;
import io.druid.segment.Segment;
import io.druid.server.DruidNode;
import io.druid.server.ServiceTypes;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

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
  private final Map<String, VersionedIntervalTimeline<ServerSelector>> timelines;
  private final ConcurrentMap<TimelineCallback, Executor> timelineCallbacks = new ConcurrentHashMap<>();

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper smileMapper;
  private final ObjectMapper customSmileMapper;
  private final HttpClient httpClient;
  private final FilteredServerInventoryView baseView;
  private final ServiceEmitter emitter;
  private final BrokerIOConfig ioConfig;
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
      @EscalatedClient HttpClient httpClient,
      FilteredServerInventoryView baseView,
      ServiceEmitter emitter,
      final BrokerSegmentWatcherConfig segmentWatcherConfig,
      BrokerIOConfig ioConfig,
      @Processing ExecutorService backgroundExecutorService
  )
  {
    this.node = DruidServer.of(node, "broker");
    this.conglomerate = conglomerate;
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.smileMapper = smileMapper;
    this.customSmileMapper = JodaStuff.overrideForInternal(smileMapper);
    this.httpClient = httpClient;
    this.baseView = baseView;
    this.emitter = emitter;
    this.backgroundExecutorService = backgroundExecutorService;
    this.clients = Maps.newConcurrentMap();
    this.selectors = Maps.newHashMap();
    this.timelines = Maps.newHashMap();
    this.ioConfig = ioConfig;

    this.segmentFilter = new Predicate<Pair<DruidServerMetadata, DataSegment>>()
    {
      @Override
      public boolean apply(
          Pair<DruidServerMetadata, DataSegment> input
      )
      {
        if (segmentWatcherConfig.getWatchedTiers() != null &&
            !segmentWatcherConfig.getWatchedTiers().contains(input.lhs.getTier())) {
          return false;
        }

        if (segmentWatcherConfig.getWatchedDataSources() != null &&
            !segmentWatcherConfig.getWatchedDataSources().contains(input.rhs.getDataSource())) {
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
            executeCallbacks(TimelineCallback::timelineInitialized);
            return ServerView.CallbackAction.CONTINUE;
          }
        },
        segmentFilter
    );

    baseView.registerServerCallback(
        exec,
        new ServerCallback()
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

      for (ServerSelector selector : selectors.values()) {
        selector.clear();
      }
      selectors.clear();
    }
  }

  private DirectDruidClient makeDirectClient(DruidServer server)
  {
    return new DirectDruidClient(
        warehouse,
        queryWatcher,
        smileMapper,
        customSmileMapper,
        httpClient,
        server.getHost(),
        server.getType(),
        emitter,
        ioConfig,
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
      selector = new ServerSelector(segment);

      VersionedIntervalTimeline<ServerSelector> timeline = timelines.get(segment.getDataSource());
      if (timeline == null) {
        timeline = new VersionedIntervalTimeline<>();
        timelines.put(segment.getDataSource(), timeline);
      }

      timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpecWithDefault().createChunk(selector));
      selectors.put(segmentId, selector);
    }

    QueryableDruidServer druidServer = getQueryableDruidServer(server);

    selector.addServerAndUpdateSegment(druidServer, segment);

    if (!timelineCallbacks.isEmpty()) {
      executeCallbacks(callback -> callback.segmentAdded(server.getMetadata(), segment));
    }
    return druidServer;
  }

  private QueryableDruidServer getQueryableDruidServer(DruidServer server)
  {
    QueryableDruidServer druidServer = clients.get(server.getName());
    if (druidServer == null) {
      DirectDruidClient client = server.equals(node) ? null : makeDirectClient(server);
      QueryableDruidServer retVal = new QueryableDruidServer(server, client);
      QueryableDruidServer exists = clients.put(server.getName(), retVal);
      if (exists != null) {
        log.warn("QueryRunner for server[%s] already existed!? Well it's getting replaced", server);
      }
      druidServer = retVal;
    }
    return druidServer;
  }

  public boolean addLocalDataSource(String dataSource)
  {
    log.debug("Adding local dataSource[%s]", dataSource);
    synchronized (lock) {
      if (timelines.containsKey(dataSource)) {
        throw new IAE("Conflicting local datasource name %s", dataSource);
      }
      return getQueryableDruidServer(node).addLocalDataSource(dataSource);
    }
  }

  public void addLocalSegment(DataSegment segment, QueryableIndex index, Map<String, Object> metaData)
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
      VersionedIntervalTimeline<LocalSegment> timeline = localServer.getLocalTimelineView().remove(dataSource);
      if (timeline == null) {
        return false;
      }
      for (PartitionChunk<LocalSegment> chunk : timeline.clear()) {
        try (LocalSegment segment = chunk.getObject()) {
          serverRemovedSegment(node.getMetadata(), segment.getDescriptor());
        }
        catch (IOException e) {
          log.info(e, "Failed to close local segment %s", chunk.getObject().getDescriptor());
        }
      }
      timelines.remove(dataSource);
      return true;
    }
  }

  private void serverRemovedSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    final String segmentId = segment.getIdentifier();

    log.debug("Removing segment[%s] from server[%s].", segmentId, server);

    synchronized (lock) {
      final ServerSelector selector = selectors.get(segmentId);
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
        VersionedIntervalTimeline<ServerSelector> timeline = timelines.get(segment.getDataSource());
        selectors.remove(segmentId);

        final PartitionChunk<ServerSelector> removedPartition = timeline.remove(
            segment.getInterval(), segment.getVersion(), segment.getShardSpecWithDefault().createChunk(selector)
        );

        if (removedPartition == null) {
          log.warn(
              "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
              segment.getInterval(),
              segment.getVersion()
          );
        } else if (!timelineCallbacks.isEmpty()) {
          executeCallbacks(callback -> callback.segmentRemoved(server, segment));
        }
        if (timeline.isEmpty()) {
          timelines.remove(segment.getDataSource());
        }
      }
    }
  }


  @Override
  public Iterable<String> getDataSources()
  {
    return timelines.keySet();
  }

  @Override
  public VersionedIntervalTimeline<ServerSelector> getTimeline(String dataSource)
  {
    synchronized (lock) {
      return timelines.get(dataSource);
    }
  }

  @Override
  public Iterable<ServerSelector> getSelectors(String dataSource)
  {
    synchronized (lock) {
      VersionedIntervalTimeline<ServerSelector> timeline = timelines.get(dataSource);
      if (timeline == null) {
        return ImmutableList.of();
      }
      return timeline.getAll();
    }
  }

  @Override
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
  public <T> QueryRunner<T> getQueryRunner(Query<T> query, final DruidServer server)
  {
    final QueryableDruidServer queryableServer = clients.get(server.getName());
    if (queryableServer != null && queryableServer.getClient() != null) {
      return queryableServer.asRemoteRunner();  // remote queryable nodes
    }
    if (query instanceof Query.ManagementQuery) {
      if (server.equals(node)) {
        return QueryRunnerHelper.toManagementRunner(query, conglomerate, null, smileMapper);
      }
      final QueryToolChest<T, Query<T>> toolchest = conglomerate.findFactory(query).getToolchest();
      final JavaType reference = toolchest.getResultTypeReference(query, smileMapper.getTypeFactory());
      final String prefix = ServiceTypes.TYPE_TO_RESOURCE.getOrDefault(server.getType(), server.getType());
      final String resource = String.format("druid/%s/v1/%s", prefix, query.getType());
      return (q, r) -> {
        try {
          return Sequences.of(execute(server, resource, reference));
        }
        catch (Exception e) {
          return Sequences.empty();
        }
      };
    }
    // try query from local segments
    if (queryableServer == null || !server.equals(node)) {
      return null;
    }
    return toRunner(query, queryableServer.getLocalTimelineView());
  }

  private <T> T execute(DruidServer server, String resource, JavaType resultType) throws Exception
  {
    URL url = new URL(String.format("http://%s/%s", server.getHost(), resource));
    Request request = new Request(HttpMethod.GET, url);
    StatusResponseHolder response = httpClient.go(request, new StatusResponseHandler(Charsets.UTF_8)).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while query on [%s] status[%s] content[%s]",
          url,
          response.getStatus(),
          response.getContent()
      );
    }
    return smileMapper.readValue(response.getContent(), resultType);
  }

  private <T> QueryRunner<T> toRunner(
      final Query<T> query, final Map<String, VersionedIntervalTimeline<LocalSegment>> indexMap
  )
  {
    // called from CachingClusteredClient.. it's always MultipleSpecificSegmentSpec
    final List<SegmentDescriptor> specs;
    if (query.getQuerySegmentSpec() instanceof MultipleSpecificSegmentSpec) {
      specs = ((MultipleSpecificSegmentSpec) query.getQuerySegmentSpec()).getDescriptors();
    } else if (query.getQuerySegmentSpec() instanceof DenseSegmentsSpec) {
      specs = ((DenseSegmentsSpec) query.getQuerySegmentSpec()).getDescriptors();
    } else {
      throw new UOE("Invalid segment spec for broker %s", query.getQuerySegmentSpec());
    }
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.warn("Unknown query type, [%s]", query.getClass());
      return NoopQueryRunner.instance();
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    final TimelineLookup<LocalSegment> timeline = indexMap.get(DataSources.getName(query));
    if (timeline == null) {
      return NoopQueryRunner.instance();
    }

    final List<Pair<SegmentDescriptor, LocalSegment>> segments = GuavaUtils.transform(specs, input -> {
      PartitionHolder<LocalSegment> entry = timeline.findEntry(input.getInterval(), input.getVersion());
      if (entry != null) {
        PartitionChunk<LocalSegment> chunk = entry.getChunk(input.getPartitionNumber());
        if (chunk != null) {
          return Pair.of(input, chunk.getObject());
        }
      }
      return Pair.of(input, null);
    });

    final List<Segment> targets = Lists.newArrayList();
    for (Pair<SegmentDescriptor, LocalSegment> segment : segments) {
      Segment target = segment.rhs == null ? null : segment.rhs.getBaseSegment();
      if (target != null) {
        targets.add(target);
      }
    }
    if (query.isDescending()) {
      Collections.reverse(targets);
    }
    final ExecutorService exec = Execs.singleThreaded("BrokerLocalProcessor-%s");

    final Supplier<RowResolver> resolver = RowResolver.supplier(targets, query);
    final Query<T> resolved = query.resolveQuery(resolver, true);

    final Supplier<Object> optimizer = factory.preFactoring(resolved, targets, resolver, exec);
    final CPUTimeMetricBuilder<T> reporter = new CPUTimeMetricBuilder<>(toolChest, emitter);

    final Iterable<QueryRunner<T>> queryRunners = Iterables.transform(segments, input -> {
      if (input.rhs == null) {
        return new ReportTimelineMissingSegmentQueryRunner<T>(input.lhs);
      }
      return buildAndDecorateQueryRunner(factory, input.rhs, input.lhs, optimizer, reporter);
    });

    return QueryRunners.runWith(
        resolved,
        reporter.report(
            QueryRunners.finalizeAndPostProcessing(
                toolChest.mergeResults(
                    factory.mergeRunners(resolved, exec, queryRunners, optimizer)
                ),
                toolChest,
                smileMapper
            )
        )
    );
  }

  // copied from server manager, except cache populator
  private <T> QueryRunner<T> buildAndDecorateQueryRunner(
      final QueryRunnerFactory<T, Query<T>> factory,
      final LocalSegment segment,
      final SegmentDescriptor descriptor,
      final Supplier<Object> optimizer,
      final CPUTimeMetricBuilder<T> reporter
  )
  {
    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(descriptor);
    return reporter.accumulate(
        new SpecificSegmentQueryRunner<T>(
            new MetricsEmittingQueryRunner<T>(
                emitter,
                toolChest,
                new BySegmentQueryRunner<T>(
                    toolChest,
                    segment.getIdentifier(),
                    segment.getInterval().getStart(),
                    new MetricsEmittingQueryRunner<T>(
                            emitter,
                            toolChest,
                            new ReferenceCountingSegmentQueryRunner<T>(
                                factory, segment, descriptor, optimizer
                            ),
                            QueryMetrics::reportSegmentTime,
                            queryMetrics -> queryMetrics.segment(segment.getIdentifier())
                        )
                ),
                QueryMetrics::reportSegmentAndCacheTime,
                queryMetrics -> queryMetrics.segment(segment.getIdentifier())
            ).withWaitMeasuredFromNow(),
            segmentSpec
        )
    );
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

  @Override
  public void removeSegmentCallback(SegmentCallback callback)
  {
    baseView.removeSegmentCallback(callback);
  }

  private void executeCallbacks(final Function<TimelineCallback, CallbackAction> function)
  {
    for (final Map.Entry<TimelineCallback, Executor> entry : timelineCallbacks.entrySet()) {
      entry.getValue().execute(
          () -> {
            if (function.apply(entry.getKey()) == CallbackAction.UNREGISTER) {
              timelineCallbacks.remove(entry.getKey());
            }
          }
      );
    }
  }

  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    return ImmutableList.copyOf(
        Iterables.transform(getServers(), s -> s.getServer().toImmutableDruidServer())
    );
  }
}
