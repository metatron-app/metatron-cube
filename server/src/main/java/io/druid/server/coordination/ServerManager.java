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

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.cache.Cache;
import io.druid.client.CachingQueryRunner;
import io.druid.client.cache.CacheConfig;
import io.druid.collections.CountingMap;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.BackgroundCaching;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Smile;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.CPUTimeMetricBuilder;
import io.druid.query.DataSource;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.QueryRunners;
import io.druid.query.QueryToolChest;
import io.druid.query.ReferenceCountingSegmentQueryRunner;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.RowResolver;
import io.druid.query.StorageHandler;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.ForwardingSegmentWalker;
import io.druid.server.ForwardHandler;
import io.druid.server.QueryManager;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 */
public class ServerManager implements ForwardingSegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(ServerManager.class);
  private final Object lock = new Object();
  private final QueryManager queryManager;
  private final SegmentLoader segmentLoader;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ForwardHandler forwardHandler;
  private final ServiceEmitter emitter;
  private final ExecutorService exec;
  private final ExecutorService cachingExec;
  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> dataSources;
  private final CountingMap<String> dataSourceSizes = new CountingMap<String>();
  private final CountingMap<String> dataSourceCounts = new CountingMap<String>();
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CacheConfig cacheConfig;

  @Inject
  public ServerManager(
      QueryManager queryManager,
      SegmentLoader segmentLoader,
      QueryRunnerFactoryConglomerate conglomerate,
      ForwardHandler forwardHandler,
      ServiceEmitter emitter,
      @Processing ExecutorService exec,
      @BackgroundCaching ExecutorService cachingExec,
      @Smile ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this.queryManager = queryManager;
    this.segmentLoader = segmentLoader;
    this.conglomerate = conglomerate;
    this.forwardHandler = forwardHandler;
    this.emitter = emitter;

    this.exec = exec;
    this.cachingExec = cachingExec;
    this.cache = cache;
    this.objectMapper = objectMapper;

    this.dataSources = new HashMap<>();
    this.cacheConfig = cacheConfig;

    final ThreadFactory factory = Executors.defaultThreadFactory();
    Executors.newSingleThreadScheduledExecutor(
        new ThreadFactory()
        {
          @Override
          public Thread newThread(Runnable r)
          {
            Thread thread = factory.newThread(r);
            thread.setName("QueryManager");
            return thread;
          }
        }
    ).scheduleWithFixedDelay(queryManager, 1, 1, TimeUnit.HOURS);
  }

  public Map<String, Long> getDataSourceSizes()
  {
    synchronized (dataSourceSizes) {
      return dataSourceSizes.snapshot();
    }
  }

  public Map<String, Long> getDataSourceCounts()
  {
    synchronized (dataSourceCounts) {
      return dataSourceCounts.snapshot();
    }
  }

  public boolean isSegmentCached(final DataSegment segment) throws SegmentLoadingException
  {
    return segmentLoader.isSegmentLoaded(segment);
  }

  /**
   * Load a single segment.
   *
   * @param segment segment to load
   *
   * @return true if the segment was newly loaded, false if it was already loaded
   *
   * @throws SegmentLoadingException if the segment cannot be loaded
   */
  public boolean loadSegment(final DataSegment segment) throws SegmentLoadingException
  {
    final String dataSource = segment.getDataSource();

    final Segment adapter;
    try {
      adapter = segmentLoader.getSegment(segment);
    }
    catch (SegmentLoadingException e) {
      try {
        segmentLoader.cleanup(segment);
      }
      catch (SegmentLoadingException e1) {
        // ignore
      }
      throw e;
    }

    if (adapter == null) {
      throw new SegmentLoadingException("Null adapter from loadSpec[%s]", segment.getLoadSpec());
    }

    synchronized (lock) {
      VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals = dataSources.get(dataSource);

      if (loadedIntervals == null) {
        loadedIntervals = new VersionedIntervalTimeline<>(Ordering.natural());
        dataSources.put(dataSource, loadedIntervals);
      }

      PartitionHolder<ReferenceCountingSegment> entry = loadedIntervals.findEntry(
          segment.getInterval(),
          segment.getVersion()
      );
      if ((entry != null) && (entry.getChunk(segment.getShardSpec().getPartitionNum()) != null)) {
        log.warn("Told to load a adapter for a segment[%s] that already exists", segment.getIdentifier());
        return false;
      }

      loadedIntervals.add(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpec().createChunk(new ReferenceCountingSegment(adapter))
      );
      synchronized (dataSourceSizes) {
        dataSourceSizes.add(dataSource, segment.getSize());
      }
      synchronized (dataSourceCounts) {
        dataSourceCounts.add(dataSource, 1L);
      }
      return true;
    }
  }

  public void dropSegment(final DataSegment segment) throws SegmentLoadingException
  {
    String dataSource = segment.getDataSource();
    synchronized (lock) {
      VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals = dataSources.get(dataSource);

      if (loadedIntervals == null) {
        log.info("Told to delete a queryable for a dataSource[%s] that doesn't exist.", dataSource);
        return;
      }

      PartitionChunk<ReferenceCountingSegment> removed = loadedIntervals.remove(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpec().createChunk((ReferenceCountingSegment) null)
      );
      ReferenceCountingSegment oldQueryable = (removed == null) ? null : removed.getObject();

      if (oldQueryable != null) {
        synchronized (dataSourceSizes) {
          dataSourceSizes.add(dataSource, -segment.getSize());
        }
        synchronized (dataSourceCounts) {
          dataSourceCounts.add(dataSource, -1L);
        }

        try {
          log.info("Attempting to close segment %s", segment.getIdentifier());
          oldQueryable.close();
        }
        catch (IOException e) {
          log.makeAlert(e, "Exception closing segment")
             .addData("dataSource", dataSource)
             .addData("segmentId", segment.getIdentifier())
             .emit();
        }
      } else {
        log.info(
            "Told to delete a queryable on dataSource[%s] for interval[%s] and version [%s] that I don't have.",
            dataSource,
            segment.getInterval(),
            segment.getVersion()
        );
      }
    }
    segmentLoader.cleanup(segment);
  }

  @Override
  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    if (query instanceof Query.ManagementQuery) {
      return QueryRunnerHelper.toManagementRunner(query, conglomerate, exec, objectMapper);
    }
    DataSource dataSource = query.getDataSource();
    if (!(dataSource instanceof TableDataSource)) {
      throw new UnsupportedOperationException("data source type '" + dataSource.getClass().getName() + "' unsupported");
    }
    String dataSourceName = getDataSourceName(dataSource);

    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = dataSources.get(dataSourceName);

    if (timeline == null) {
      return new NoopQueryRunner<T>();
    }

    FunctionalIterable<Pair<SegmentDescriptor, ReferenceCountingSegment>> segments = FunctionalIterable
        .create(intervals)
        .transformCat(
            new Function<Interval, Iterable<TimelineObjectHolder<String, ReferenceCountingSegment>>>()
            {
              @Override
              public Iterable<TimelineObjectHolder<String, ReferenceCountingSegment>> apply(Interval input)
              {
                return timeline.lookup(input);
              }
            }
        )
        .transformCat(
            new Function<TimelineObjectHolder<String, ReferenceCountingSegment>, Iterable<Pair<SegmentDescriptor, ReferenceCountingSegment>>>()
            {
              @Override
              public Iterable<Pair<SegmentDescriptor, ReferenceCountingSegment>> apply(
                  @Nullable
                  final TimelineObjectHolder<String, ReferenceCountingSegment> holder
              )
              {
                if (holder == null) {
                  return null;
                }

                return FunctionalIterable
                    .create(holder.getObject())
                    .transform(
                        new Function<PartitionChunk<ReferenceCountingSegment>, Pair<SegmentDescriptor, ReferenceCountingSegment>>()
                        {
                          @Override
                          public Pair<SegmentDescriptor, ReferenceCountingSegment> apply(PartitionChunk<ReferenceCountingSegment> chunk)
                          {
                            return Pair.of(
                                new SegmentDescriptor(
                                    holder.getInterval(),
                                    holder.getVersion(),
                                    chunk.getChunkNumber()
                                ), chunk.getObject()
                            );
                          }
                        }
                    );
              }
            }
        );

    return toQueryRunner(query, Lists.newArrayList(segments));
  }

  private String getDataSourceName(DataSource dataSource)
  {
    return Iterables.getOnlyElement(dataSource.getNames());
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    if (query instanceof Query.ManagementQuery) {
      return QueryRunnerHelper.toManagementRunner(query, conglomerate, exec, objectMapper);
    }
    String dataSourceName = getDataSourceName(query.getDataSource());

    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = dataSources.get(dataSourceName);
    if (timeline == null) {
      return new NoopQueryRunner<T>();
    }

    List<Pair<SegmentDescriptor, ReferenceCountingSegment>> segments = Lists.newArrayList(
        Iterables.transform(
            specs, new Function<SegmentDescriptor, Pair<SegmentDescriptor, ReferenceCountingSegment>>()
            {
              @Override
              public Pair<SegmentDescriptor, ReferenceCountingSegment> apply(SegmentDescriptor input)
              {
                PartitionHolder<ReferenceCountingSegment> entry = timeline.findEntry(
                    input.getInterval(), input.getVersion()
                );
                if (entry != null) {
                  PartitionChunk<ReferenceCountingSegment> chunk = entry.getChunk(input.getPartitionNumber());
                  if (chunk != null) {
                    return Pair.of(input, chunk.getObject());
                  }
                }
                return Pair.of(input, null);
              }
            }
        )
    );
    return toQueryRunner(query, segments);
  }

  private <T> QueryRunner<T> toQueryRunner(
      Query<T> query,
      List<Pair<SegmentDescriptor, ReferenceCountingSegment>> segments
  )
  {
    log.info("Running resolved [%s:%s] on [%d] segments", query.getType(), query.getId(), segments.size());

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.makeAlert("Unknown resolved type, [%s]", query.getClass())
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<T>();
    }
    QueryRunnerFactory.Splitable<T, Query<T>> splitable = null;
    if (factory instanceof QueryRunnerFactory.Splitable) {
      splitable = (QueryRunnerFactory.Splitable<T, Query<T>>) factory;
    }

    List<Segment> targets = Lists.newArrayList();
    List<QueryRunner<T>> missingSegments = Lists.newArrayList();
    for (Pair<SegmentDescriptor, ReferenceCountingSegment> segment : segments) {
      Segment target = segment.rhs == null ? null : segment.rhs.getBaseSegment();
      if (target != null) {
        targets.add(new Segment.WithDescriptor(segment.rhs, segment.lhs));
      } else {
        missingSegments.add(new ReportTimelineMissingSegmentQueryRunner<T>(segment.lhs));
      }
    }
    if (query.isDescending()) {
      targets = Lists.reverse(targets);
    }

    final Supplier<RowResolver> resolver = RowResolver.supplier(targets, query);
    final Query<T> resolved = query.resolveQuery(resolver, objectMapper);

    final Future<Object> optimizer = factory.preFactoring(resolved, targets, resolver, exec);

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    final CPUTimeMetricBuilder<T> reporter = new CPUTimeMetricBuilder<T>(toolChest.makeMetricBuilder(), emitter);

    final Function<Iterable<Segment>, QueryRunner<T>> function = new Function<Iterable<Segment>, QueryRunner<T>>()
    {
      @Override
      public QueryRunner<T> apply(Iterable<Segment> segments)
      {
        Iterable<QueryRunner<T>> runners = Iterables.transform(
            segments, buildAndDecorateQueryRunner(factory, optimizer, reporter)
        );
        return FinalizeResultsQueryRunner.finalize(
            toolChest.mergeResults(
                factory.mergeRunners(exec, runners, optimizer)
            ),
            toolChest,
            objectMapper
        );
      }
    };
    if (splitable != null) {
      List<List<Segment>> splits = splitable.splitSegments(resolved, targets, optimizer, resolver, this);
      if (!GuavaUtils.isNullOrEmpty(splits)) {
        log.info("Split segments into %d groups", splits.size());
        return reporter.report(
            QueryRunners.concat(Iterables.concat(missingSegments, Iterables.transform(splits, function)))
        );
      }
    }

    QueryRunner<T> runner = QueryRunners.concat(GuavaUtils.concat(missingSegments, function.apply(targets)));
    if (splitable != null) {
      Iterable<Query<T>> splits = splitable.splitQuery(resolved, targets, optimizer, resolver, this);
      if (splits != null) {
        return reporter.report(toConcatRunner(splits, runner));
      }
    }
    return QueryRunners.runWith(resolved, reporter.report(runner));
  }

  private <T> QueryRunner<T> toConcatRunner(final Iterable<Query<T>> queries, final QueryRunner<T> runner)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> baseQuery, final Map<String, Object> responseContext)
      {
        // stop streaming if canceled
        final Execs.TaggedFuture future = Execs.tag(new Execs.SettableFuture(), "split-runner");
        queryManager.registerQuery(baseQuery, future);
        return Sequences.withBaggage(
            Sequences.interruptible(future, Sequences.concat(
                Iterables.transform(
                    queries, new Function<Query<T>, Sequence<T>>()
                    {
                      @Override
                      public Sequence<T> apply(final Query<T> splitQuery)
                      {
                        return runner.run(splitQuery, responseContext);
                      }
                    }
                )
            )),
            future
        );
      }
    };
  }

  private <T> Function<Segment, QueryRunner<T>> buildAndDecorateQueryRunner(
      final QueryRunnerFactory<T, Query<T>> factory,
      final Future<Object> optimizer,
      final CPUTimeMetricBuilder<T> reporter
  )
  {
    return new Function<Segment, QueryRunner<T>>()
    {
      @Override
      public QueryRunner<T> apply(final Segment segment)
      {
        final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
        final Segment adapter = ((Segment.WithDescriptor) segment).getSegment();
        final SegmentDescriptor descriptor = ((Segment.WithDescriptor) segment).getDescriptor();
        final SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(descriptor);
        return reporter.accumulate(
            new SpecificSegmentQueryRunner<T>(
                new MetricsEmittingQueryRunner<T>(
                    emitter,
                    reporter,
                    new BySegmentQueryRunner<T>(
                        adapter.getIdentifier(),
                        adapter.getDataInterval().getStart(),
                        new CachingQueryRunner<T>(
                            adapter.getIdentifier(),
                            descriptor,
                            objectMapper,
                            cache,
                            toolChest,
                            new MetricsEmittingQueryRunner<T>(
                                emitter,
                                toolChest.makeMetricBuilder(),
                                new ReferenceCountingSegmentQueryRunner<T>(
                                    factory,
                                    (ReferenceCountingSegment) adapter,
                                    descriptor,
                                    optimizer
                                ),
                                "query/segment/time",
                                ImmutableMap.of("segment", adapter.getIdentifier())
                            ),
                            cachingExec,
                            cacheConfig
                        )
                    ),
                    "query/segmentAndCache/time",
                    ImmutableMap.of("segment", adapter.getIdentifier())
                ).withWaitMeasuredFromNow(),
                segmentSpec
            )
        );
      }
    };
  }

  @Override
  public StorageHandler getHandler(String scheme)
  {
    return forwardHandler.getHandler(scheme);
  }

  @Override
  public <T> QueryRunner<T> wrap(Query<T> query, QueryRunner<T> baseRunner)
  {
    return forwardHandler.wrapForward(query, baseRunner);
  }
}
