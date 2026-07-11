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
import io.druid.query.BaseQuery;
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
import io.druid.segment.LazySegment;
import io.druid.segment.RangePrefetch;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.query.filter.DimFilter;
import io.druid.segment.loading.RangeBufferTracker;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.loading.SegmentPruneIndex;
import io.druid.server.ForwardHandler;
import io.druid.server.QueryManager;
import io.druid.timeline.DataSegment;
import io.druid.timeline.SegmentKey;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.segment.QueryableIndex;
import io.druid.java.util.common.JodaUtils;
import io.druid.timeline.partition.PartitionHolder;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 */
public class ServerManager implements ForwardingSegmentWalker, QuerySegmentWalker.DenseSupport, ResidencyManager.Host
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

  // Residency policy (auto loadMode) — reconciles tmpfs<->range; this class supplies the mechanism (Host).
  private final ResidencyManager residencyManager;

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

    if (segmentLoader.residencyManaged()) {
      this.residencyManager = new ResidencyManager(this);   // loadMode=auto
      residencyManager.start();
    } else {
      this.residencyManager = null;
    }
  }

  public Map<String, Long> getDataSourceSizes()
  {
    return dataSourceSizes;
  }

  public Map<String, Integer> getDataSourceCounts()
  {
    return dataSourceCounts;
  }

  /**
   * A loaded segment's {@link QueryableIndex} for {@code dataSource} (any one — the schema is uniform across a
   * datasource's segments), or null if nothing is loaded. Range-served segments return a header-first index (its
   * column capabilities come from the segment header, so a schema read costs no column scan beyond one lucene head).
   * Backs the {@code /schema} endpoint the Trino connector uses to discover columns + their secondary indexes.
   */
  public QueryableIndex getRepresentativeIndex(String dataSource)
  {
    synchronized (lock) {
      final VersionedIntervalTimeline<ReferenceCountingSegment> tl = dataSources.get(dataSource);
      if (tl == null) {
        return null;
      }
      for (TimelineObjectHolder<ReferenceCountingSegment> holder :
          tl.lookup(new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT))) {
        for (PartitionChunk<ReferenceCountingSegment> chunk : holder.getObject()) {
          final QueryableIndex index = chunk.getObject().asQueryableIndex(false);
          if (index != null) {
            return index;
          }
        }
      }
      return null;
    }
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
   * @return loaded segment
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
          segment.getShardSpecWithDefault().createChunk(withIdleFree(new ReferenceCountingSegment(adapter), segment))
      );
      synchronized (dataSourceSizes) {
        dataSourceSizes.addTo(dataSource, segment.getSize());
      }
      synchronized (dataSourceCounts) {
        dataSourceCounts.addTo(dataSource, 1);
      }
    }
    // Use the row count without forcing a load: a LazySegment answers this from its descriptor, so a
    // range-served (header-first) segment is NOT materialized at load time — only on the first query.
    final int numRows = adapter.getNumRows();
    if (numRows >= 0) {
      return segment.withNumRows(numRows);
    }
    return segment;
  }

  /**
   * Wire a range segment's idle callback to the range-buffer tracker: when this segment's last query reference is
   * released, the tracker frees its off-heap column buffers deterministically (under memory pressure) instead of
   * leaving them for the Cleaner. No-op when range residency isn't bounded (tracker null) or the segment is a
   * downloaded/tmpfs one the tracker doesn't manage.
   */
  private ReferenceCountingSegment withIdleFree(ReferenceCountingSegment rcs, DataSegment segment)
  {
    final RangeBufferTracker tracker = segmentLoader.rangeTracker();
    if (tracker != null) {
      final String id = segment.getIdentifier();
      rcs.setOnIdle(() -> tracker.onIdle(id));
    }
    return rcs;
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

  // --- ResidencyManager.Host: mechanism (timeline swap + cache sizing) for the residency policy ---

  @Override
  public long localUsedBytes()
  {
    return segmentLoader.localUsedBytes();
  }

  @Override
  public long localMaxBytes()
  {
    return segmentLoader.localMaxBytes();
  }

  /** Partition currently-loaded range-capable segments into tmpfs (downloaded) and range (LazySegment). */
  @Override
  public void snapshotResident(List<ReferenceCountingSegment> tmpfs, List<ReferenceCountingSegment> range)
  {
    synchronized (lock) {
      for (VersionedIntervalTimeline<ReferenceCountingSegment> tl : dataSources.values()) {
        for (ReferenceCountingSegment rcs : tl.getAll()) {
          final Segment base = rcs.getBaseSegment();
          if (base != null) {
            (base instanceof LazySegment ? range : tmpfs).add(rcs);
          }
        }
      }
    }
  }

  /** Swap a tmpfs-resident segment to a range (off-heap) LazySegment in-place, then free its tmpfs files. */
  @Override
  public boolean demoteToRange(final DataSegment segment)
  {
    try {
      final Segment ranged = segmentLoader.getRangeSegment(segment);
      if (!(ranged instanceof LazySegment)) {
        return false;   // not range-capable (e.g. s3_zip) — leave it downloaded
      }
      final ReferenceCountingSegment newRcs = withIdleFree(new ReferenceCountingSegment(ranged), segment);
      final ReferenceCountingSegment old;
      synchronized (lock) {
        final VersionedIntervalTimeline<ReferenceCountingSegment> tl = dataSources.get(segment.getDataSource());
        if (tl == null) {
          return false;
        }
        final PartitionChunk<ReferenceCountingSegment> removed = tl.remove(
            segment.getInterval(), segment.getVersion(), segment.getShardSpecWithDefault().createChunk(null));
        old = removed == null ? null : removed.getObject();
        if (old == null) {
          return false;   // dropped concurrently
        }
        tl.add(segment.getInterval(), segment.getVersion(), segment.getShardSpecWithDefault().createChunk(newRcs));
      }
      old.close();                      // closes when in-flight queries release their references
      segmentLoader.cleanup(segment);   // free tmpfs (Linux keeps mmap pages until the last unmap)
      log.info("[residency] demoted segment[%s] tmpfs -> range", segment.getIdentifier());
      return true;
    }
    catch (Throwable t) {
      log.warn(t, "[residency] demote failed for [%s]", segment.getIdentifier());
      return false;
    }
  }

  /** Swap a range (off-heap) segment to a downloaded (tmpfs mmap) one in-place. Caller ensures tmpfs has room. */
  @Override
  public boolean promoteToTmpfs(final DataSegment segment)
  {
    try {
      final Segment downloaded = segmentLoader.getDownloadedSegment(segment);   // downloads to the tmpfs cache
      if (downloaded instanceof LazySegment) {
        return false;   // couldn't download (unexpected)
      }
      final ReferenceCountingSegment newRcs = new ReferenceCountingSegment(downloaded);
      final ReferenceCountingSegment old;
      synchronized (lock) {
        final VersionedIntervalTimeline<ReferenceCountingSegment> tl = dataSources.get(segment.getDataSource());
        if (tl == null) {
          return false;
        }
        final PartitionChunk<ReferenceCountingSegment> removed = tl.remove(
            segment.getInterval(), segment.getVersion(), segment.getShardSpecWithDefault().createChunk(null));
        old = removed == null ? null : removed.getObject();
        if (old == null) {
          return false;   // dropped concurrently
        }
        tl.add(segment.getInterval(), segment.getVersion(), segment.getShardSpecWithDefault().createChunk(newRcs));
      }
      old.close();   // old range LazySegment; its direct column buffers are freed by their Cleaner on release
      log.info("[residency] promoted segment[%s] range -> tmpfs", segment.getIdentifier());
      return true;
    }
    catch (Throwable t) {
      log.warn(t, "[residency] promote failed for [%s]", segment.getIdentifier());
      return false;
    }
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
            Iterables.filter(GuavaUtils.explode(intervals, timeline::lookup), Predicates.notNull()),
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

    // Segment pruning: if enabled, skip any segment whose resident value index proves the query's filter can't
    // match it — before its columns are ever range-fetched from deep storage.
    final SegmentPruneIndex pruneIndex = segmentLoader.pruneIndex();
    final DimFilter pruneFilter = query instanceof Query.FilterSupport ? ((Query.FilterSupport<?>) query).getFilter() : null;

    List<Segment> targets = Lists.newArrayList();
    List<QueryRunner<T>> missingSegments = Lists.newArrayList();
    int tmpfsCount = 0, rangeCount = 0, prunedCount = 0;   // residency of the segments THIS query touches (range == LazySegment)
    for (Pair<SegmentDescriptor, ReferenceCountingSegment> segment : segments) {
      Segment target = segment.rhs == null ? null : segment.rhs.getBaseSegment();
      if (target != null) {
        if (pruneIndex != null && pruneFilter != null && pruneIndex.canSkip(target.getIdentifier(), pruneFilter)) {
          prunedCount++;   // filter provably can't match -> no column fetch for this segment
          continue;
        }
        targets.add(Segments.withLimit(segment.rhs, segment.lhs));
        if (target instanceof LazySegment) {
          rangeCount++;
        } else {
          tmpfsCount++;
        }
      } else {
        missingSegments.add(new ReportTimelineMissingSegmentQueryRunner<>(segment.lhs));
      }
    }
    final int tmpfsResident = tmpfsCount, rangeResident = rangeCount, missingCount = missingSegments.size();
    final int prunedResident = prunedCount;
    if (query.isDescending()) {
      targets = Lists.reverse(targets);
    }

    final Supplier<RowResolver> resolver = RowResolver.supplier(targets, query);
    final Query<T> resolved = factory.prepare(query.resolveQuery(resolver, true), resolver);

    final Supplier<Object> optimizer = factory.preFactoring(resolved, targets, resolver, exec);

    final QueryToolChest<T> toolChest = factory.getToolchest();
    final CPUTimeMetricBuilder<T> reporter = new CPUTimeMetricBuilder<>(toolChest, emitter);

    final Function<Iterable<Segment>, QueryRunner<T>> function = new Function<>()
    {
      @Override
      public QueryRunner<T> apply(Iterable<Segment> targets)
      {
        Iterable<QueryRunner<T>> runners = Iterables.transform(
            targets, s -> buildAndDecorateQueryRunner(s, factory, optimizer, reporter)
        );
        return QueryRunners.finalizeAndPostProcessing(
            toolChest.mergeResults(
                localizeDirect(factory.mergeRunners(resolved, exec, runners, optimizer))
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
        return withResidencyLog(QueryRunners.runWith(resolved, reporter.report(
            QueryRunners.concat(Iterables.concat(missingSegments, Iterables.transform(splits, function)))
        )), query, tmpfsResident, rangeResident, missingCount, prunedResident);
      }
    }

    QueryRunner<T> runner = QueryRunners.concat(GuavaUtils.concat(missingSegments, function.apply(targets)));
    if (splitable != null) {
      List<Query<T>> splits = splitable.splitQuery(resolved, targets, optimizer, resolver, this);
      if (!GuavaUtils.isNullOrEmpty(splits)) {
        return withResidencyLog(reporter.report(toConcatRunner(splits, runner)), query, tmpfsResident, rangeResident, missingCount, prunedResident);
      }
    }
    return withResidencyLog(QueryRunners.runWith(resolved, reporter.report(runner)), query, tmpfsResident, rangeResident, missingCount, prunedResident);
  }

  /**
   * A directly-queried historical (:8083, not routed through the broker) receives a query with {@code #brokerSide}
   * defaulting to true, so {@code toolChest.mergeResults} takes the broker branch (a final CompactRow merge). But
   * the local segment merge ({@code mergeRunners}) with finalize=true emits MapBasedRow, which the Compact-expecting
   * merge then can't cast (ClassCastException). Mirror what the broker does for its data nodes: feed the LOCAL merge
   * the localized query ({@code toLocalQuery()} -> brokerSide/finalize off, so it emits unfinalized CompactRow) while
   * the outer mergeResults keeps the original and does the final compact-merge + finalize. Broker-routed queries are
   * already localized (brokerSide=false), so they pass through unchanged.
   */
  /** Wrap the query's runner so that WHEN THE QUERY FINISHES it logs the residency of the segments it touched. */
  private <T> QueryRunner<T> withResidencyLog(
      final QueryRunner<T> runner, final Query<T> query, final int tmpfs, final int range, final int missing,
      final int pruned
  )
  {
    if (query.getContextBoolean(Query.DISABLE_LOG, false)) {
      return runner;
    }
    return (q, responseContext) -> Sequences.withBaggage(
        runner.run(q, responseContext),
        () -> {
          log.info(
              "[residency] query[%s] touched %d segment(s): %d tmpfs, %d range%s%s",
              query.getId(), tmpfs + range, tmpfs, range, missing > 0 ? ", " + missing + " missing" : "",
              pruned > 0 ? ", " + pruned + " pruned" : ""
          );
          log.info("[lucene-prof] query[%s] %s", query.getId(), io.druid.java.util.common.RangeProf.snapshotAndReset());
        }
    );
  }

  private static <T> QueryRunner<T> localizeDirect(final QueryRunner<T> localMerge)
  {
    return (query, responseContext) ->
        localMerge.run(BaseQuery.isBrokerSide(query) ? query.toLocalQuery() : query, responseContext);
  }

  private <T> QueryRunner<T> toConcatRunner(
      final List<Query<T>> queries,
      final QueryRunner<T> runner
  )
  {
    if (queries.size() == 1) {
      return QueryRunners.runWith(queries.getFirst(), runner);
    }
    return (resolved, responseContext) ->
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

    final QueryRunner<T> built = reporter.accumulate(
        new SpecificSegmentQueryRunner<>(
            new MetricsEmittingQueryRunner<>(
                emitter,
                toolChest,
                new BySegmentQueryRunner<>(
                    toolChest,
                    segment.getIdentifier(),
                    segment.getInterval().getStart(),
                    new CachingQueryRunner<>(
                        segment.getIdentifier(),
                        segmentSpec.getDescriptor(),
                        objectMapper,
                        cache,
                        toolChest,
                        new MetricsEmittingQueryRunner<>(
                            emitter,
                            toolChest,
                            new ReferenceCountingSegmentQueryRunner<>(
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

    // Range-served (header-first) segment: prefetch the query's referenced columns in parallel before the scan,
    // so N cold column range-GETs overlap instead of running one at a time on the query's critical path. No-op
    // for mmap segments (LazySegment is only produced by the range-serve loader).
    final LazySegment lazy = Segments._unwrap(segment, LazySegment.class);
    if (lazy == null) {
      return built;
    }
    return (query, responseContext) -> {
      try {
        RangePrefetch.warm(lazy.asQueryableIndex(false), query.estimatedInitialColumns());
      }
      catch (Throwable t) {
        log.debug("range-prefetch skipped for [%s]: %s", segment.getIdentifier(), t.toString());
      }
      return built.run(query, responseContext);
    };
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
