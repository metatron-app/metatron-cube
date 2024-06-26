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

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.cache.Cache;
import io.druid.client.CachingQueryRunner;
import io.druid.client.cache.CacheConfig;
import io.druid.collections.IntList;
import io.druid.collections.String2IntMap;
import io.druid.collections.String2LongMap;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.BackgroundCaching;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.UOE;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.CPUTimeMetricBuilder;
import io.druid.query.DataSource;
import io.druid.query.DataSources;
import io.druid.query.ForwardingSegmentWalker;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.ReferenceCountingSegmentQueryRunner;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.RowResolver;
import io.druid.query.SegmentDescriptor;
import io.druid.query.StorageHandler;
import io.druid.query.TableDataSource;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.ForwardHandler;
import io.druid.server.QueryManager;
import io.druid.timeline.DataSegment;
import io.druid.timeline.SegmentKey;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class ServerManager implements ForwardingSegmentWalker, QuerySegmentWalker.DenseSupport
{
  private static final EmittingLogger log = new EmittingLogger(ServerManager.class);

  private static final long CHECK_INTERVAL = 60;  // 1 minute

  private final Object lock = new Object();
  private final QueryManager queryManager;
  private final SegmentLoader segmentLoader;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ForwardHandler forwardHandler;
  private final ServiceEmitter emitter;
  private final ExecutorService exec;
  private final ExecutorService cachingExec;
  private final Map<String, VersionedIntervalTimeline<ReferenceCountingSegment>> dataSources;
  private final String2LongMap dataSourceSizes = new String2LongMap();
  private final String2IntMap dataSourceCounts = new String2IntMap();
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

    queryManager.start(CHECK_INTERVAL);
  }

  public Map<String, Long> getDataSourceSizes()
  {
    return dataSourceSizes;
  }

  public Map<String, Integer> getDataSourceCounts()
  {
    return dataSourceCounts;
  }

  public boolean isEmpty()
  {
    synchronized (lock) {
      return dataSources.isEmpty();
    }
  }

  public void done()
  {
    segmentLoader.done();
  }

  public boolean isSegmentLoaded(final DataSegment segment) throws SegmentLoadingException
  {
    return segmentLoader.isLoaded(segment);
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
  public DataSegment loadSegment(final DataSegment segment) throws SegmentLoadingException
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
      VersionedIntervalTimeline<ReferenceCountingSegment> loadedIntervals = dataSources.get(dataSource);

      if (loadedIntervals == null) {
        loadedIntervals = new VersionedIntervalTimeline<>();
        dataSources.put(dataSource, loadedIntervals);
      }

      PartitionHolder<ReferenceCountingSegment> entry = loadedIntervals.findEntry(
          segment.getInterval(),
          segment.getVersion()
      );
      if (entry != null && entry.getChunk(segment.getShardSpecWithDefault().getPartitionNum()) != null) {
        log.warn("Told to load a adapter for a segment[%s] that already exists", segment.getIdentifier());
        return null;
      }

      loadedIntervals.add(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpecWithDefault().createChunk(new ReferenceCountingSegment(adapter))
      );
      synchronized (dataSourceSizes) {
        dataSourceSizes.addTo(dataSource, segment.getSize());
      }
      synchronized (dataSourceCounts) {
        dataSourceCounts.addTo(dataSource, 1);
      }
    }
    final QueryableIndex index = adapter.asQueryableIndex(false);
    if (index != null) {
      return segment.withNumRows(index.getNumRows());
    }
    return segment;
  }

  public void dropSegment(final DataSegment segment) throws SegmentLoadingException
  {
    String dataSource = segment.getDataSource();
    synchronized (lock) {
      VersionedIntervalTimeline<ReferenceCountingSegment> loadedIntervals = dataSources.get(dataSource);

      if (loadedIntervals == null) {
        log.info("Told to delete a queryable for a dataSource[%s] that doesn't exist.", dataSource);
        return;
      }

      PartitionChunk<ReferenceCountingSegment> removed = loadedIntervals.remove(
          segment.getInterval(),
          segment.getVersion(),
          segment.getShardSpecWithDefault().createChunk(null)
      );
      ReferenceCountingSegment oldQueryable = (removed == null) ? null : removed.getObject();

      if (oldQueryable != null) {
        synchronized (dataSourceSizes) {
          dataSourceSizes.addTo(dataSource, -segment.getSize());
        }
        synchronized (dataSourceCounts) {
          dataSourceCounts.addTo(dataSource, -1);
        }

        try {
          log.debug("Attempting to close segment %s", segment.getIdentifier());
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
      if (loadedIntervals.isEmpty()) {
        dataSources.remove(dataSource);
      }
    }
    segmentLoader.cleanup(segment);
  }

  @Override
  public QueryConfig getConfig()
  {
    return conglomerate.getConfig();
  }

  @Override
  public ExecutorService getExecutor()
  {
    return exec;
  }

  @Override
  public ObjectMapper getMapper()
  {
    return objectMapper;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    if (query instanceof Query.ManagementQuery) {
      return QueryRunnerHelper.toManagementRunner(query, conglomerate, exec, objectMapper);
    }

    final QueryRunnerFactory<T> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T> toolChest = factory.getToolchest();
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    DataSource dataSource = query.getDataSource();
    if (!(dataSource instanceof TableDataSource)) {
      throw new UOE("data source type '%s' unsupported", dataSource.getClass());
    }
    final String dataSourceName = DataSources.getName(query);

    final VersionedIntervalTimeline<ReferenceCountingSegment> timeline = dataSources.get(dataSourceName);

    if (timeline == null) {
      return NoopQueryRunner.instance();
    }

    Iterable<Pair<SegmentDescriptor, ReferenceCountingSegment>> segments =
        GuavaUtils.explode(
            Iterables.filter(GuavaUtils.explode(intervals, i -> timeline.lookup(i)), Predicates.notNull()),
            holder -> Iterables.transform(
                holder.getObject(),
                chunk -> Pair.of(
                    new SegmentDescriptor(
                        dataSourceName, holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
                    ),
                    chunk.getObject()
                )
            )
        );

    return toQueryRunner(query, Lists.newArrayList(segments));
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    if (query instanceof Query.ManagementQuery) {
      return QueryRunnerHelper.toManagementRunner(query, conglomerate, exec, objectMapper);
    }
    String dataSourceName = DataSources.getName(query);

    final VersionedIntervalTimeline<ReferenceCountingSegment> timeline = dataSources.get(dataSourceName);
    if (timeline == null) {
      return NoopQueryRunner.instance();
    }

    Iterable<Pair<SegmentDescriptor, ReferenceCountingSegment>> segments = Iterables.transform(
        specs,
        input -> {
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
    );
    return toQueryRunner(query, Lists.newArrayList(segments));
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, List<SegmentKey> keys, List<IntList> partitions)
  {
    String dataSourceName = DataSources.getName(query.getDataSource());
    VersionedIntervalTimeline<ReferenceCountingSegment> timeline = dataSources.get(dataSourceName);
    if (timeline == null) {
      return NoopQueryRunner.instance();
    }
    List<Pair<SegmentDescriptor, ReferenceCountingSegment>> segments = Lists.newArrayList();
    for (int i = 0; i < keys.size(); i++) {
      SegmentKey key = keys.get(i);
      PartitionHolder<ReferenceCountingSegment> entry = timeline.findEntry(key.getInterval(), key.getVersion());

      IntIterator iterator = partitions.get(i).intIterator();
      while (iterator.hasNext()) {
        int partition = iterator.nextInt();
        SegmentDescriptor desc = new SegmentDescriptor(dataSourceName, key.getInterval(), key.getVersion(), partition);
        PartitionChunk<ReferenceCountingSegment> chunk = entry == null ? null : entry.getChunk(partition);
        segments.add(Pair.of(desc, chunk == null ? null : chunk.getObject()));
      }
    }
    return toQueryRunner(query, segments);
  }

  private <T> QueryRunner<T> toQueryRunner(
      Query<T> query,
      List<Pair<SegmentDescriptor, ReferenceCountingSegment>> segments
  )
  {
    if (!query.getContextBoolean(Query.DISABLE_LOG, false)) {
      log.info(
          "Running resolved [%s][%s:%s] on [%d] segments",
          query.getId(),
          query.getType(),
          query.getDataSource(),
          segments.size()
      );
    }

    final QueryRunnerFactory<T> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.makeAlert("Unknown resolved type, [%s]", query.getClass())
         .addData("dataSource", query.getDataSource())
         .emit();
      return NoopQueryRunner.instance();
    }
    QueryRunnerFactory.Splitable<T> splitable = null;
    if (factory instanceof QueryRunnerFactory.Splitable) {
      splitable = (QueryRunnerFactory.Splitable<T>) factory;
    }

    List<Segment> targets = Lists.newArrayList();
    List<QueryRunner<T>> missingSegments = Lists.newArrayList();
    for (Pair<SegmentDescriptor, ReferenceCountingSegment> segment : segments) {
      Segment target = segment.rhs == null ? null : segment.rhs.getBaseSegment();
      if (target != null) {
        targets.add(Segments.withLimit(segment.rhs, segment.lhs));
      } else {
        missingSegments.add(new ReportTimelineMissingSegmentQueryRunner<T>(segment.lhs));
      }
    }
    if (query.isDescending()) {
      targets = Lists.reverse(targets);
    }

    final Supplier<RowResolver> resolver = RowResolver.supplier(targets, query);
    final Query<T> resolved = factory.prepare(query.resolveQuery(resolver, true), resolver);

    final Supplier<Object> optimizer = factory.preFactoring(resolved, targets, resolver, exec);

    final QueryToolChest<T> toolChest = factory.getToolchest();
    final CPUTimeMetricBuilder<T> reporter = new CPUTimeMetricBuilder<T>(toolChest, emitter);

    final Function<Iterable<Segment>, QueryRunner<T>> function = new Function<Iterable<Segment>, QueryRunner<T>>()
    {
      @Override
      public QueryRunner<T> apply(Iterable<Segment> segments)
      {
        Iterable<QueryRunner<T>> runners = Iterables.transform(
            segments, s -> buildAndDecorateQueryRunner(s, factory, optimizer, reporter)
        );
        return QueryRunners.finalizeAndPostProcessing(
            toolChest.mergeResults(
                factory.mergeRunners(resolved, exec, runners, optimizer)
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
        return QueryRunners.runWith(resolved, reporter.report(
            QueryRunners.concat(Iterables.concat(missingSegments, Iterables.transform(splits, function)))
        ));
      }
    }

    QueryRunner<T> runner = QueryRunners.concat(GuavaUtils.concat(missingSegments, function.apply(targets)));
    if (splitable != null) {
      List<Query<T>> splits = splitable.splitQuery(resolved, targets, optimizer, resolver, this);
      if (!GuavaUtils.isNullOrEmpty(splits)) {
        return reporter.report(toConcatRunner(splits, runner));
      }
    }
    return QueryRunners.runWith(resolved, reporter.report(runner));
  }

  private <T> QueryRunner<T> toConcatRunner(
      final List<Query<T>> queries,
      final QueryRunner<T> runner
  )
  {
    if (queries.size() == 1) {
      return QueryRunners.runWith(queries.get(0), runner);
    }
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> resolved, final Map<String, Object> responseContext)
      {
        // stop streaming if canceled
        final Execs.TaggedFuture future = Execs.tag(new Execs.SettableFuture<>(), "split-runner");
        queryManager.register(resolved, future);
        return Sequences.withBaggage(
            Sequences.interruptible(future, Sequences.concat(
                resolved.estimatedOutputColumns(),
                Iterables.transform(queries, query -> runner.run(query, responseContext))
            )),
            future
        );
      }
    };
  }

  private <T> QueryRunner<T> buildAndDecorateQueryRunner(
      final Segment segment,
      final QueryRunnerFactory<T> factory,
      final Supplier<Object> optimizer,
      final CPUTimeMetricBuilder<T> reporter
  )
  {
    final QueryToolChest<T> toolChest = factory.getToolchest();
    final SpecificSegmentSpec segmentSpec = segment.asSpec();

    return reporter.accumulate(
        new SpecificSegmentQueryRunner<T>(
            new MetricsEmittingQueryRunner<T>(
                emitter,
                toolChest,
                new BySegmentQueryRunner<T>(
                    toolChest,
                    segment.getIdentifier(),
                    segment.getInterval().getStart(),
                    new CachingQueryRunner<T>(
                        segment.getIdentifier(),
                        segmentSpec.getDescriptor(),
                        objectMapper,
                        cache,
                        toolChest,
                        new MetricsEmittingQueryRunner<T>(
                            emitter,
                            toolChest,
                            new ReferenceCountingSegmentQueryRunner<T>(
                                factory,
                                Segments.unwrap(segment, ReferenceCountingSegment.class),
                                segmentSpec.getDescriptor(),
                                optimizer
                            ),
                            QueryMetrics::reportSegmentTime,
                            queryMetrics -> queryMetrics.segment(segment.getIdentifier())
                        ),
                        cachingExec,
                        cacheConfig
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
  public StorageHandler getHandler(String scheme)
  {
    return forwardHandler.getHandler(scheme);
  }

  @Override
  public <T> QueryRunner<T> handle(Query<T> query, QueryRunner<T> baseRunner)
  {
    return forwardHandler.wrapForward(query, baseRunner);
  }
}
