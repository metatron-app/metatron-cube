/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 SK Telecom Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.cache.Cache;
import io.druid.client.CachingQueryRunner;
import io.druid.client.cache.CacheConfig;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.common.utils.StringUtils;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.CPUTimeMetricQueryRunner;
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
import io.druid.query.QueryToolChest;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.RowResolver;
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.plumber.Sink;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 */
public class AppenderatorImpl implements Appenderator
{
  private static final EmittingLogger log = new EmittingLogger(AppenderatorImpl.class);
  private static final int WARN_DELAY = 1000;
  private static final String IDENTIFIER_FILE_NAME = "identifier.json";
  private static final String CONTEXT_SKIP_INCREMENTAL_SEGMENT = "skipIncrementalSegment";

  private final DataSchema schema;
  private final AppenderatorConfig tuningConfig;
  private final FireDepartmentMetrics metrics;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final ServiceEmitter emitter;
  private final ExecutorService queryExecutorService;
  private final IndexIO indexIO;
  private final IndexMerger indexMerger;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final Map<SegmentIdentifier, Sink> sinks = Maps.newConcurrentMap();
  private final Set<SegmentIdentifier> droppingSinks = Sets.newConcurrentHashSet();
  private final VersionedIntervalTimeline<Sink> sinkTimeline = new VersionedIntervalTimeline<>();
  // This variable updated in add(), persist(), and drop()
  private final AtomicInteger rowsCurrentlyInMemory = new AtomicInteger();
  private final AtomicInteger totalRows = new AtomicInteger();
  // Synchronize persisting commitMetadata so that multiple persist threads (if present)
  // and abandon threads do not step over each other
  private final Lock commitLock = new ReentrantLock();

  private volatile ListeningExecutorService persistExecutor = null;
  private volatile ListeningExecutorService pushExecutor = null;
  // use intermediate executor so that deadlock conditions can be prevented
  // where persist and push Executor try to put tasks in each other queues
  // thus creating circular dependency
  private volatile ListeningExecutorService intermediateTempExecutor = null;
  private volatile long nextFlush;
  private volatile FileLock basePersistDirLock = null;
  private volatile FileChannel basePersistDirLockChannel = null;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public AppenderatorImpl(
      DataSchema schema,
      AppenderatorConfig tuningConfig,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ServiceEmitter emitter,
      ExecutorService queryExecutorService,
      IndexIO indexIO,
      IndexMerger indexMerger,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this.schema = Preconditions.checkNotNull(schema, "schema");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.metrics = Preconditions.checkNotNull(metrics, "metrics");
    this.dataSegmentPusher = Preconditions.checkNotNull(dataSegmentPusher, "dataSegmentPusher");
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
    this.conglomerate = conglomerate;
    this.segmentAnnouncer = segmentAnnouncer;
    this.emitter = emitter;
    this.queryExecutorService = queryExecutorService;
    this.indexIO = indexIO;
    this.indexMerger = indexMerger;
    this.cache = cache;
    this.cacheConfig = cacheConfig;

    if (conglomerate != null) {
      // If we're not querying (no conglomerate) then it's ok for the other query stuff to be null.
      // But otherwise, we need them all.
      Preconditions.checkNotNull(segmentAnnouncer, "segmentAnnouncer");
      Preconditions.checkNotNull(emitter, "emitter");
      Preconditions.checkNotNull(queryExecutorService, "queryExecutorService");
      Preconditions.checkNotNull(cache, "cache");
      Preconditions.checkNotNull(cacheConfig, "cacheConfig");

      if (!cache.isLocal()) {
        log.error("Configured cache is not local, caching will not be enabled");
      }
    }

    log.info("Creating appenderator for dataSource[%s]", schema.getDataSource());
  }

  @Override
  public String getDataSource()
  {
    return schema.getDataSource();
  }

  @Override
  public Object startJob()
  {
    tuningConfig.getBasePersistDirectory().mkdirs();
    lockBasePersistDirectory();
    final Object retVal = bootstrapSinksFromDisk();
    initializeExecutors();
    resetNextFlush();
    return retVal;
  }

  @Override
  public AppenderatorAddResult add(
      final SegmentIdentifier identifier,
      final InputRow row,
      final Supplier<Committer> committerSupplier,
      final boolean allowIncrementalPersists
  ) throws IndexSizeExceededException, SegmentNotWritableException
  {
    if (!identifier.getDataSource().equals(schema.getDataSource())) {
      throw new IAE(
          "Expected dataSource[%s] but was asked to insert row for dataSource[%s]?!",
          schema.getDataSource(),
          identifier.getDataSource()
      );
    }

    final Sink sink = getOrCreateSink(identifier);
    final int sinkRowsInMemoryBeforeAdd = sink.getNumRowsInMemory();
    final int sinkRowsInMemoryAfterAdd;

    try {
      sinkRowsInMemoryAfterAdd = sink.add(row);
    }
    catch (IndexSizeExceededException e) {
      // Uh oh, we can't do anything about this! We can't persist (commit metadata would be out of sync) and we
      // can't add the row (it just failed). This should never actually happen, though, because we check
      // sink.canAddRow after returning from add.
      log.error(e, "Sink for segment[%s] was unexpectedly full!", identifier);
      throw e;
    }

    if (sinkRowsInMemoryAfterAdd < 0) {
      throw new SegmentNotWritableException("Attempt to add row to swapped-out sink for segment[%s].", identifier);
    }

    final int numAddedRows = sinkRowsInMemoryAfterAdd - sinkRowsInMemoryBeforeAdd;
    rowsCurrentlyInMemory.addAndGet(numAddedRows);
    totalRows.addAndGet(numAddedRows);

    boolean isPersistRequired = false;
    if (!sink.canAppendRow()
        || System.currentTimeMillis() > nextFlush
        || rowsCurrentlyInMemory.get() >= tuningConfig.getMaxRowsInMemory()) {
      if (allowIncrementalPersists) {
        // persistAll clears rowsCurrentlyInMemory, no need to update it.
        persistAll(committerSupplier.get());
      } else {
        isPersistRequired = true;
      }
    }

    return new AppenderatorAddResult(identifier, sink.getNumRows(), isPersistRequired);
  }

  @Override
  public List<SegmentIdentifier> getSegments()
  {
    return ImmutableList.copyOf(sinks.keySet());
  }

  @Override
  public int getRowCount(final SegmentIdentifier identifier)
  {
    final Sink sink = sinks.get(identifier);

    if (sink == null) {
      throw new ISE("No such sink: %s", identifier);
    } else {
      return sink.getNumRows();
    }
  }

  @Override
  public int getTotalRowCount()
  {
    return totalRows.get();
  }

  @VisibleForTesting
  int getRowsInMemory()
  {
    return rowsCurrentlyInMemory.get();
  }

  private Sink getOrCreateSink(final SegmentIdentifier identifier)
  {
    Sink retVal = sinks.get(identifier);

    if (retVal == null) {
      retVal = new Sink(
          identifier.getInterval(),
          schema,
          identifier.getShardSpec(),
          identifier.getVersion(),
          tuningConfig,
          objectMapper
      );

      try {
        segmentAnnouncer.announceSegment(retVal.getSegment());
      }
      catch (IOException e) {
        log.makeAlert(e, "Failed to announce new segment[%s]", schema.getDataSource())
           .addData("interval", retVal.getInterval())
           .emit();
      }

      sinks.put(identifier, retVal);
      sinkTimeline.add(retVal.getInterval(), retVal.getVersion(), identifier.getShardSpec().createChunk(retVal));
    }

    return retVal;
  }

  @Override
  public QueryConfig getConfig()
  {
    return conglomerate.getConfig();
  }

  @Override
  public ExecutorService getExecutor()
  {
    return queryExecutorService;
  }

  @Override
  public ObjectMapper getMapper()
  {
    return objectMapper;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    if (conglomerate == null) {
      throw new IllegalStateException("Don't query me, bro.");
    }

    final String dataSource = schema.getDataSource();
    final Iterable<SegmentDescriptor> specs = GuavaUtils.explode(
        GuavaUtils.explode(
            intervals, interval -> sinkTimeline.lookup(interval)
        ),
        holder -> Iterables.transform(
            holder.getObject(),
            chunk -> new SegmentDescriptor(
                dataSource, holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
            )
        )
    );

    return getQueryRunnerForSegments(query, specs);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    if (query instanceof Query.ManagementQuery) {
      return QueryRunnerHelper.toManagementRunner(query, conglomerate, null, objectMapper);
    }
    if (conglomerate == null) {
      throw new IllegalStateException("Don't query me, bro.");
    }

    // We only handle one dataSource. Make sure it's in the list of names, then ignore from here on out.
    if (!query.getDataSource().getNames().contains(getDataSource())) {
      log.makeAlert("Received query for unknown dataSource")
         .addData("dataSource", query.getDataSource())
         .emit();
      return NoopQueryRunner.instance();
    }

    return toQueryRunner(query, specs);
  }

  private <T> QueryRunner<T> toQueryRunner(final Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final QueryRunnerFactory<T> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T> toolchest = factory.getToolchest();
    final boolean skipIncrementalSegment = query.getContextValue(CONTEXT_SKIP_INCREMENTAL_SEGMENT, false);

    final List<SegmentDescriptor> descriptors = Lists.newArrayList(specs);
    final List<Sink> sinks = Lists.newArrayList(
        Iterables.transform(
            descriptors, new Function<SegmentDescriptor, Sink>()
            {
              @Override
              public Sink apply(final SegmentDescriptor descriptor)
              {
                final PartitionHolder<Sink> holder = sinkTimeline.findEntry(
                    descriptor.getInterval(),
                    descriptor.getVersion()
                );
                if (holder == null) {
                  return null;
                }
                final PartitionChunk<Sink> chunk = holder.getChunk(descriptor.getPartitionNumber());
                if (chunk == null) {
                  return null;
                }
                return chunk.getObject();
              }
            }
        )
    );
    final List<Segment> segments = Lists.newArrayList(
        Iterables.concat(
            Iterables.transform(
                Iterables.filter(sinks, Predicates.notNull()), new Function<Sink, Iterable<Segment>>()
                {
                  @Override
                  public Iterable<Segment> apply(Sink input)
                  {
                    List<Segment> segmentList = Lists.newArrayList();
                    for (FireHydrant hydrant : input) {
                      if (!skipIncrementalSegment || hydrant.hasSwapped()) {
                        segmentList.add(hydrant.getSegment());
                      }
                    }
                    return segmentList;
                  }
                }
            )
        )
    );
    if (query.isDescending()) {
      Collections.reverse(segments);
    }
    final Supplier<RowResolver> resolver = RowResolver.supplier(segments, query);
    final Query<T> resolved = query.resolveQuery(resolver, true);
    final Supplier<Object> optimizer = factory.preFactoring(resolved, segments, resolver, queryExecutorService);

    final List<Pair<SegmentDescriptor, Sink>> targets = GuavaUtils.zip(descriptors, sinks);

    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    return QueryRunners.runWith(resolved, CPUTimeMetricQueryRunner.safeBuild(
        toolchest.mergeResults(
            factory.mergeRunners(
                resolved,
                queryExecutorService,
                Iterables.transform(
                    specs,
                    new Function<SegmentDescriptor, QueryRunner<T>>()
                    {
                      @Override
                      public QueryRunner<T> apply(final SegmentDescriptor descriptor)
                      {
                        final PartitionHolder<Sink> holder = sinkTimeline.findEntry(
                            descriptor.getInterval(),
                            descriptor.getVersion()
                        );
                        if (holder == null) {
                          return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
                        }

                        final PartitionChunk<Sink> chunk = holder.getChunk(descriptor.getPartitionNumber());
                        if (chunk == null) {
                          return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
                        }

                        final Sink theSink = chunk.getObject();

                        return new SpecificSegmentQueryRunner<>(
                            withPerSinkMetrics(
                                new BySegmentQueryRunner<>(
                                    toolchest,
                                    theSink.getIdentifier(),
                                    descriptor.getInterval().getStart(),
                                    factory.mergeRunners(
                                        resolved,
                                        Execs.newDirectExecutorService(),
                                        Iterables.transform(
                                            theSink,
                                            new Function<FireHydrant, QueryRunner<T>>()
                                            {
                                              @Override
                                              public QueryRunner<T> apply(final FireHydrant hydrant)
                                              {
                                                // Hydrant might swap at any point, but if it's swapped at the start
                                                // then we know it's *definitely* swapped.
                                                final boolean hydrantDefinitelySwapped = hydrant.hasSwapped();

                                                if (skipIncrementalSegment && !hydrantDefinitelySwapped) {
                                                  return NoopQueryRunner.instance();
                                                }

                                                // Prevent the underlying segment from swapping when its being iterated
                                                final Pair<Segment, Closeable> segment = hydrant.getAndIncrementSegment();
                                                try {
                                                  QueryRunner<T> baseRunner = QueryRunners.withResource(
                                                      factory.createRunner(segment.lhs, optimizer),
                                                      segment.rhs
                                                  );

                                                  // 1) Only use caching if data is immutable
                                                  // 2) Hydrants are not the same between replicas, make sure cache is local
                                                  if (hydrantDefinitelySwapped && cache.isLocal()) {
                                                    return new CachingQueryRunner<>(
                                                        makeHydrantCacheIdentifier(hydrant, segment.lhs),
                                                        descriptor,
                                                        objectMapper,
                                                        cache,
                                                        toolchest,
                                                        baseRunner,
                                                        Execs.newDirectExecutorService(),
                                                        cacheConfig
                                                    );
                                                  } else {
                                                    return baseRunner;
                                                  }
                                                }
                                                catch (RuntimeException e) {
                                                  CloseQuietly.close(segment.rhs);
                                                  throw e;
                                                }
                                              }
                                            }
                                        ),
                                        optimizer
                                    )
                                ),
                                toolchest,
                                theSink.getIdentifier(),
                                cpuTimeAccumulator
                            ),
                            new SpecificSegmentSpec(descriptor)
                        );
                      }
                    }
                ),
                optimizer
            )
        ),
        toolchest,
        emitter,
        cpuTimeAccumulator,
        true
    ));
  }

  /**
   * Decorates a Sink's query runner to emit query/segmentAndCache/time, query/segment/time, query/wait/time once
   * each for the whole Sink. Also adds CPU time to cpuTimeAccumulator.
   */
  private <T> QueryRunner<T> withPerSinkMetrics(
      final QueryRunner<T> sinkRunner,
      final QueryToolChest<T> queryToolChest,
      final String sinkSegmentIdentifier,
      final AtomicLong cpuTimeAccumulator
  )
  {

    // Note: reportSegmentAndCacheTime and reportSegmentTime are effectively the same here. They don't split apart
    // cache vs. non-cache due to the fact that Sinks may be partially cached and partially uncached. Making this
    // better would need to involve another accumulator like the cpuTimeAccumulator that we could share with the
    // sinkRunner.

    return CPUTimeMetricQueryRunner.safeBuild(
        new MetricsEmittingQueryRunner<>(
            emitter,
            queryToolChest,
            new MetricsEmittingQueryRunner<>(
                emitter,
                queryToolChest,
                sinkRunner,
                QueryMetrics::reportSegmentTime,
                queryMetrics -> queryMetrics.segment(sinkSegmentIdentifier)
            ),
            QueryMetrics::reportSegmentAndCacheTime,
            queryMetrics -> queryMetrics.segment(sinkSegmentIdentifier)
        ).withWaitMeasuredFromNow(),
        queryToolChest,
        emitter,
        cpuTimeAccumulator,
        false
    );
  }

  @Override
  public void clear() throws InterruptedException
  {
    // Drop commit metadata, then abandon all segments.

    try {
      final ListenableFuture<?> uncommitFuture = persistExecutor.submit(
          new Callable<Object>()
          {
            @Override
            public Object call() throws Exception
            {
              try {
                commitLock.lock();
                objectMapper.writeValue(computeCommitFile(), Committed.nil());
              }
              finally {
                commitLock.unlock();
              }
              return null;
            }
          }
      );

      // Await uncommit.
      uncommitFuture.get();

      // Drop everything.
      final List<ListenableFuture<?>> futures = Lists.newArrayList();
      for (Map.Entry<SegmentIdentifier, Sink> entry : sinks.entrySet()) {
        futures.add(abandonSegment(entry.getKey(), entry.getValue(), true));
      }

      // Await dropping.
      Futures.allAsList(futures).get();
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ListenableFuture<?> drop(final SegmentIdentifier identifier)
  {
    final Sink sink = sinks.get(identifier);
    if (sink != null) {
      return abandonSegment(identifier, sink, true);
    } else {
      return Futures.immediateFuture(null);
    }
  }

  @Override
  public ListenableFuture<Object> persist(Collection<SegmentIdentifier> identifiers, Committer committer)
  {
    final Map<String, Integer> currentHydrants = Maps.newHashMap();
    final List<Pair<FireHydrant, SegmentIdentifier>> indexesToPersist = Lists.newArrayList();
    int numPersistedRows = 0;
    for (SegmentIdentifier identifier : identifiers) {
      final Sink sink = sinks.get(identifier);
      if (sink == null) {
        throw new ISE("No sink for identifier: %s", identifier);
      }
      final List<FireHydrant> hydrants = Lists.newArrayList(sink);
      currentHydrants.put(identifier.getIdentifierAsString(), hydrants.size());
      numPersistedRows += sink.getNumRowsInMemory();

      final int limit = sink.isWritable() ? hydrants.size() - 1 : hydrants.size();

      for (FireHydrant hydrant : hydrants.subList(0, limit)) {
        if (!hydrant.hasSwapped()) {
          log.info("Hydrant[%s] hasn't persisted yet, persisting. Segment[%s]", hydrant, identifier);
          indexesToPersist.add(Pair.of(hydrant, identifier));
        }
      }

      if (sink.swappable()) {
        indexesToPersist.add(Pair.of(sink.swap(), identifier));
      }
    }

    log.info("Submitting persist runnable for dataSource[%s]", schema.getDataSource());

    final String threadName = String.format("%s-incremental-persist", schema.getDataSource());
    final Object commitMetadata = committer.getMetadata();
    final Stopwatch runExecStopwatch = Stopwatch.createStarted();
    final Stopwatch persistStopwatch = Stopwatch.createStarted();
    final ListenableFuture<Object> future = persistExecutor.submit(
        new ThreadRenamingCallable<Object>(threadName)
        {
          @Override
          public Object doCall()
          {
            try {
              for (Pair<FireHydrant, SegmentIdentifier> pair : indexesToPersist) {
                metrics.incrementRowOutputCount(persistHydrant(pair.lhs, pair.rhs));
              }

              log.info(
                  "Committing metadata[%s] for sinks[%s].", commitMetadata, Joiner.on(", ").join(
                      currentHydrants.entrySet()
                                     .stream()
                                     .map(entry -> StringUtils.format(
                                         "%s:%d",
                                         entry.getKey(),
                                         entry.getValue()
                                     ))
                                     .collect(Collectors.toList())
                  )
              );

              committer.run();

              try {
                commitLock.lock();
                final File commitFile = computeCommitFile();
                final Map<String, Integer> commitHydrants = Maps.newHashMap();
                if (commitFile.exists()) {
                  // merge current hydrants with existing hydrants
                  final Committed oldCommitted = objectMapper.readValue(commitFile, Committed.class);
                  commitHydrants.putAll(oldCommitted.getHydrants());
                }
                commitHydrants.putAll(currentHydrants);
                objectMapper.writeValue(commitFile, new Committed(commitHydrants, commitMetadata));
              }
              finally {
                commitLock.unlock();
              }

              return commitMetadata;
            }
            catch (Exception e) {
              metrics.incrementFailedPersists();
              throw Throwables.propagate(e);
            }
            finally {
              metrics.incrementNumPersists();
              metrics.incrementPersistTimeMillis(persistStopwatch.elapsed(TimeUnit.MILLISECONDS));
              persistStopwatch.stop();
            }
          }
        }
    );

    final long startDelay = runExecStopwatch.elapsed(TimeUnit.MILLISECONDS);
    metrics.incrementPersistBackPressureMillis(startDelay);
    if (startDelay > WARN_DELAY) {
      log.warn("Ingestion was throttled for [%,d] millis because persists were pending.", startDelay);
    }
    runExecStopwatch.stop();
    resetNextFlush();

    // NB: The rows are still in memory until they're done persisting, but we only count rows in active indexes.
    rowsCurrentlyInMemory.addAndGet(-numPersistedRows);

    return future;
  }

  @Override
  public ListenableFuture<Object> persistAll(final Committer committer)
  {
    // Submit persistAll task to the persistExecutor
    return persist(sinks.keySet(), committer);
  }

  @Override
  public ListenableFuture<SegmentsAndMetadata> push(
      final Collection<SegmentIdentifier> identifiers,
      final Committer committer
  )
  {
    final Map<SegmentIdentifier, Sink> theSinks = Maps.newHashMap();
    for (final SegmentIdentifier identifier : identifiers) {
      final Sink sink = sinks.get(identifier);
      if (sink == null) {
        throw new ISE("No sink for identifier: %s", identifier);
      }
      theSinks.put(identifier, sink);
      sink.finishWriting();
    }

    return Futures.transform(
        persist(identifiers, committer),
        (Function<Object, SegmentsAndMetadata>) commitMetadata -> {
          final List<DataSegment> dataSegments = Lists.newArrayList();

          for (Map.Entry<SegmentIdentifier, Sink> entry : theSinks.entrySet()) {
            if (droppingSinks.contains(entry.getKey())) {
              log.info("Skipping push of currently-dropping sink[%s]", entry.getKey());
              continue;
            }

            final DataSegment dataSegment = mergeAndPush(entry.getKey(), entry.getValue());
            if (dataSegment != null) {
              dataSegments.add(dataSegment);
            } else {
              log.warn("mergeAndPush[%s] returned null, skipping.", entry.getKey());
            }
          }

          return new SegmentsAndMetadata(dataSegments, commitMetadata);
        },
        pushExecutor
    );
  }

  /**
   * Insert a barrier into the merge-and-push queue. When this future resolves, all pending pushes will have finished.
   * This is useful if we're going to do something that would otherwise potentially break currently in-progress
   * pushes.
   */
  private ListenableFuture<?> pushBarrier()
  {
    return intermediateTempExecutor.submit(
        (Runnable) () -> pushExecutor.submit(() -> {})
    );
  }

  /**
   * Merge segment, push to deep storage. Should only be used on segments that have been fully persisted. Must only
   * be run in the single-threaded pushExecutor.
   *
   * @param identifier sink identifier
   * @param sink       sink to push
   *
   * @return segment descriptor, or null if the sink is no longer valid
   */

  private DataSegment mergeAndPush(final SegmentIdentifier identifier, final Sink sink)
  {
    // Bail out if this sink is null or otherwise not what we expect.
    if (sinks.get(identifier) != sink) {
      log.warn("Sink for segment[%s] no longer valid, bailing out of mergeAndPush.", identifier);
      return null;
    }

    if (sink.isWritable()) {
      throw new ISE("Expected sink to be no longer writable before mergeAndPush. Segment[%s].", identifier);
    }

    // Use a descriptor file to indicate that pushing has completed.
    final File persistDir = computePersistDir(identifier);
    final File mergedTarget = new File(persistDir, "merged");
    final File descriptorFile = computeDescriptorFile(identifier);

    // Sanity checks
    for (FireHydrant hydrant : sink) {
      synchronized (hydrant) {
        if (!hydrant.hasSwapped()) {
          throw new ISE("Expected sink to be fully persisted before mergeAndPush. Segment[%s].", identifier);
        }
      }
    }

    try {
      if (descriptorFile.exists()) {
        // Already pushed.
        log.info("Segment[%s] already pushed.", identifier);
        return objectMapper.readValue(descriptorFile, DataSegment.class);
      }

      log.info("Pushing merged index for segment[%s].", identifier);

      removeDirectory(mergedTarget);

      if (mergedTarget.exists()) {
        throw new ISE("Merged target[%s] exists after removing?!", mergedTarget);
      }

      List<QueryableIndex> indexes = Lists.newArrayList();
      for (FireHydrant fireHydrant : sink) {
        Segment segment = fireHydrant.getSegment();
        final QueryableIndex queryableIndex = segment.asQueryableIndex(false);
        log.info("Adding hydrant[%s]", fireHydrant);
        indexes.add(queryableIndex);
      }

      final File mergedFile = indexMerger.mergeQueryableIndex(
          indexes,
          schema.getGranularitySpec().isRollup(),
          schema.getAggregators(),
          mergedTarget,
          tuningConfig.getIndexSpec()
      );

      final DataSegment template = indexIO.decorateMeta(sink.getSegment(), mergedFile);
      // Retry pushing segments because uploading to deep storage might fail especially for cloud storage types
      final DataSegment segment = RetryUtils.retry(
          () -> dataSegmentPusher.push(mergedFile, template),
          exception -> exception instanceof Exception,
          5
      );

      objectMapper.writeValue(descriptorFile, segment);

      log.info("Pushed merged index for segment[%s], descriptor is: %s", identifier, segment);

      return segment;
    }
    catch (Exception e) {
      metrics.incrementFailedHandoffs();
      log.warn(e, "Failed to push merged index for segment[%s].", identifier);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close()
  {
    if (!closed.compareAndSet(false, true)) {
      log.info("Appenderator already closed");
      return;
    }

    log.info("Shutting down...");

    final List<ListenableFuture<?>> futures = Lists.newArrayList();
    for (Map.Entry<SegmentIdentifier, Sink> entry : sinks.entrySet()) {
      futures.add(abandonSegment(entry.getKey(), entry.getValue(), false));
    }

    try {
      Futures.allAsList(futures).get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn(e, "Interrupted during close()");
    }
    catch (ExecutionException e) {
      log.warn(e, "Unable to abandon existing segments during close()");
    }

    try {
      shutdownExecutors();
      Preconditions.checkState(
          persistExecutor == null || persistExecutor.awaitTermination(365, TimeUnit.DAYS),
          "persistExecutor not terminated"
      );
      Preconditions.checkState(
          pushExecutor == null || pushExecutor.awaitTermination(365, TimeUnit.DAYS),
          "pushExecutor not terminated"
      );
      Preconditions.checkState(
          intermediateTempExecutor == null || intermediateTempExecutor.awaitTermination(365, TimeUnit.DAYS),
          "intermediateTempExecutor not terminated"
      );
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ISE("Failed to shutdown executors during close()");
    }

    // Only unlock if executors actually shut down.
    unlockBasePersistDirectory();
  }

  /**
   * Unannounce the segments and wait for outstanding persists to finish.
   * Do not unlock base persist dir as we are not waiting for push executor to shut down
   * relying on current JVM to shutdown to not cause any locking problem if the task is restored.
   * In case when task is restored and current task is still active because of push executor (which it shouldn't be
   * since push executor starts daemon threads) then the locking should fail and new task should fail to start.
   * This also means that this method should only be called when task is shutting down.
   */
  @Override
  public void closeNow()
  {
    if (!closed.compareAndSet(false, true)) {
      log.info("Appenderator already closed");
      return;
    }

    log.info("Shutting down immediately...");
    for (Sink sink : sinks.values()) {
      try {
        segmentAnnouncer.unannounceSegment(sink.getSegment());
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to unannounce segment[%s]", schema.getDataSource())
           .addData("identifier", sink.getIdentifier())
           .emit();
      }
    }
    try {
      shutdownExecutors();
      Preconditions.checkState(
          persistExecutor == null || persistExecutor.awaitTermination(365, TimeUnit.DAYS),
          "persistExecutor not terminated"
      );
      Preconditions.checkState(
          intermediateTempExecutor == null || intermediateTempExecutor.awaitTermination(365, TimeUnit.DAYS),
          "intermediateTempExecutor not terminated"
      );
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ISE("Failed to shutdown executors during close()");
    }
  }

  private void lockBasePersistDirectory()
  {
    if (basePersistDirLock == null) {
      try {
        basePersistDirLockChannel = FileChannel.open(
            computeLockFile().toPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
        );

        basePersistDirLock = basePersistDirLockChannel.tryLock();
        if (basePersistDirLock == null) {
          throw new ISE("Cannot acquire lock on basePersistDir: %s", computeLockFile());
        }
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private void unlockBasePersistDirectory()
  {
    try {
      if (basePersistDirLock != null) {
        basePersistDirLock.release();
        basePersistDirLockChannel.close();
        basePersistDirLock = null;
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void initializeExecutors()
  {
    final int maxPendingPersists = tuningConfig.getMaxPendingPersists();

    if (persistExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      persistExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded(
              "appenderator_persist_%d", maxPendingPersists
          )
      );
    }
    if (pushExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      pushExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded(
              "appenderator_merge_%d", 1
          )
      );
    }
    if (intermediateTempExecutor == null) {
      // use single threaded executor with SynchronousQueue so that all abandon operations occur sequentially
      intermediateTempExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded(
              "appenderator_abandon_%d", 0
          )
      );
    }
  }

  private void shutdownExecutors()
  {
    if (persistExecutor != null) {
      persistExecutor.shutdownNow();
    }
    if (pushExecutor != null) {
      pushExecutor.shutdownNow();
    }
    if (intermediateTempExecutor != null) {
      intermediateTempExecutor.shutdownNow();
    }
  }

  private void resetNextFlush()
  {
    nextFlush = new DateTime().plus(tuningConfig.getIntermediatePersistPeriod()).getMillis();
  }

  /**
   * Populate "sinks" and "sinkTimeline" with committed segments, and announce them with the segmentAnnouncer.
   *
   * @return persisted commit metadata
   */
  private Object bootstrapSinksFromDisk()
  {
    Preconditions.checkState(sinks.isEmpty(), "Already bootstrapped?!");

    final File baseDir = tuningConfig.getBasePersistDirectory();
    if (!baseDir.exists()) {
      return null;
    }

    final File[] files = baseDir.listFiles();
    if (files == null) {
      return null;
    }


    final Committed committed;
    File commitFile = null;
    try {
      commitLock.lock();
      commitFile = computeCommitFile();
      if (commitFile.exists()) {
        committed = objectMapper.readValue(commitFile, Committed.class);
      } else {
        committed = Committed.nil();
      }
    }
    catch (Exception e) {
      throw new ISE(e, "Failed to read commitFile: %s", commitFile);
    }
    finally {
      commitLock.unlock();
    }

    log.info("Loading sinks from[%s]: %s", baseDir, committed.getHydrants().keySet());

    for (File sinkDir : files) {
      final File identifierFile = new File(sinkDir, IDENTIFIER_FILE_NAME);
      if (!identifierFile.isFile()) {
        // No identifier in this sinkDir; it must not actually be a sink directory. Skip it.
        continue;
      }

      try {
        final SegmentIdentifier identifier = objectMapper.readValue(
            new File(sinkDir, "identifier.json"),
            SegmentIdentifier.class
        );

        final int committedHydrants = committed.getCommittedHydrants(identifier.getIdentifierAsString());

        if (committedHydrants <= 0) {
          log.info("Removing uncommitted sink at [%s]", sinkDir);
          FileUtils.deleteDirectory(sinkDir);
          continue;
        }

        // To avoid reading and listing of "merged" dir and other special files
        final File[] sinkFiles = sinkDir.listFiles(
            new FilenameFilter()
            {
              @Override
              public boolean accept(File dir, String fileName)
              {
                return !(Ints.tryParse(fileName) == null);
              }
            }
        );

        Arrays.sort(
            sinkFiles,
            new Comparator<File>()
            {
              @Override
              public int compare(File o1, File o2)
              {
                return Integer.compare(Integer.parseInt(o1.getName()), Integer.parseInt(o2.getName()));
              }
            }
        );

        DataSegment template = identifier.toDataSegment();
        List<FireHydrant> hydrants = Lists.newArrayList();
        for (File hydrantDir : sinkFiles) {
          final int hydrantNumber = Integer.parseInt(hydrantDir.getName());

          if (hydrantNumber >= committedHydrants) {
            log.info("Removing uncommitted segment at [%s]", hydrantDir);
            FileUtils.deleteDirectory(hydrantDir);
          } else {
            log.info("Loading previously persisted segment at [%s]", hydrantDir);
            if (hydrantNumber != hydrants.size()) {
              throw new ISE("Missing hydrant [%,d] in sinkDir [%s].", hydrants.size(), sinkDir);
            }

            QueryableIndex index = indexIO.loadIndex(hydrantDir);
            DataSegment segment = template.withDimensions(Lists.newArrayList(index.getAvailableDimensions()))
                                          .withMetrics(Lists.newArrayList(index.getAvailableMetrics()))
                                          .withNumRows(index.getNumRows());
            hydrants.add(
                new FireHydrant(
                    new QueryableIndexSegment(index, segment, hydrantNumber),
                    hydrantDir,
                    hydrantNumber
                )
            );
          }
        }

        // Make sure we loaded enough hydrants.
        if (committedHydrants != hydrants.size()) {
          throw new ISE("Missing hydrant [%,d] in sinkDir [%s].", hydrants.size(), sinkDir);
        }

        Sink currSink = new Sink(
            identifier.getInterval(),
            schema,
            identifier.getShardSpec(),
            identifier.getVersion(),
            tuningConfig,
            hydrants,
            objectMapper
        );
        sinks.put(identifier, currSink);
        sinkTimeline.add(
            currSink.getInterval(),
            currSink.getVersion(),
            identifier.getShardSpec().createChunk(currSink)
        );

        segmentAnnouncer.announceSegment(currSink.getSegment());
      }
      catch (IOException e) {
        log.makeAlert(e, "Problem loading sink[%s] from disk.", schema.getDataSource())
           .addData("sinkDir", sinkDir)
           .emit();
      }
    }

    // Make sure we loaded all committed sinks.
    final Set<String> loadedSinks = Sets.newHashSet(
        Iterables.transform(
            sinks.keySet(),
            new Function<SegmentIdentifier, String>()
            {
              @Override
              public String apply(SegmentIdentifier input)
              {
                return input.getIdentifierAsString();
              }
            }
        )
    );
    final Set<String> missingSinks = Sets.difference(committed.getHydrants().keySet(), loadedSinks);
    if (!missingSinks.isEmpty()) {
      throw new ISE("Missing committed sinks [%s]", Joiner.on(", ").join(missingSinks));
    }

    return committed.getMetadata();
  }

  private ListenableFuture<?> abandonSegment(
      final SegmentIdentifier identifier,
      final Sink sink,
      final boolean removeOnDiskData
  )
  {
    // Ensure no future writes will be made to this sink.
    sink.finishWriting();

    // Mark this identifier as dropping, so no future push tasks will pick it up.
    droppingSinks.add(identifier);

    // Decrement this sink's rows from rowsCurrentlyInMemory (we only count active sinks).
    rowsCurrentlyInMemory.addAndGet(-sink.getNumRowsInMemory());
    totalRows.addAndGet(-sink.getNumRows());

    // Wait for any outstanding pushes to finish, then abandon the segment inside the persist thread.
    return Futures.transform(
        pushBarrier(),
        new Function<Object, Object>()
        {
          @Nullable
          @Override
          public Object apply(@Nullable Object input)
          {
            if (sinks.get(identifier) != sink) {
              // Only abandon sink if it is the same one originally requested to be abandoned.
              log.warn("Sink for segment[%s] no longer valid, not abandoning.", identifier);
              return null;
            }

            if (removeOnDiskData) {
              // Remove this segment from the committed list. This must be done from the persist thread.
              log.info("Removing commit metadata for segment[%s].", identifier);
              try {
                commitLock.lock();
                final File commitFile = computeCommitFile();
                if (commitFile.exists()) {
                  final Committed oldCommitted = objectMapper.readValue(commitFile, Committed.class);
                  objectMapper.writeValue(commitFile, oldCommitted.without(identifier.getIdentifierAsString()));
                }
              }
              catch (Exception e) {
                log.makeAlert(e, "Failed to update committed segments[%s]", schema.getDataSource())
                   .addData("identifier", identifier.getIdentifierAsString())
                   .emit();
                throw Throwables.propagate(e);
              }
              finally {
                commitLock.unlock();
              }
            }

            // Unannounce the segment.
            try {
              segmentAnnouncer.unannounceSegment(sink.getSegment());
            }
            catch (Exception e) {
              log.makeAlert(e, "Failed to unannounce segment[%s]", schema.getDataSource())
                 .addData("identifier", identifier.getIdentifierAsString())
                 .emit();
            }

            log.info("Removing sink for segment[%s].", identifier);
            sinks.remove(identifier);
            droppingSinks.remove(identifier);
            sinkTimeline.remove(
                sink.getInterval(),
                sink.getVersion(),
                identifier.getShardSpec().createChunk(sink)
            );

            if (removeOnDiskData) {
              removeDirectory(computePersistDir(identifier));
            }

            return null;
          }
        },
        // use persistExecutor to make sure that all the pending persists completes before
        // starting to abandon segments
        persistExecutor
    );
  }

  private File computeCommitFile()
  {
    return new File(tuningConfig.getBasePersistDirectory(), "commit.json");
  }

  private File computeLockFile()
  {
    return new File(tuningConfig.getBasePersistDirectory(), ".lock");
  }

  private File computePersistDir(SegmentIdentifier identifier)
  {
    return new File(tuningConfig.getBasePersistDirectory(), identifier.getIdentifierAsString());
  }

  private File computeIdentifierFile(SegmentIdentifier identifier)
  {
    return new File(computePersistDir(identifier), IDENTIFIER_FILE_NAME);
  }

  private File computeDescriptorFile(SegmentIdentifier identifier)
  {
    return new File(computePersistDir(identifier), "descriptor.json");
  }

  private File createPersistDirIfNeeded(SegmentIdentifier identifier) throws IOException
  {
    final File persistDir = computePersistDir(identifier);
    if (!persistDir.mkdir() && !persistDir.exists()) {
      throw new IOException(String.format("Could not create directory: %s", persistDir));
    }

    objectMapper.writeValue(computeIdentifierFile(identifier), identifier);

    return persistDir;
  }

  /**
   * Persists the given hydrant and returns the number of rows persisted. Must only be called in the single-threaded
   * persistExecutor.
   *
   * @param indexToPersist hydrant to persist
   * @param identifier     the segment this hydrant is going to be part of
   *
   * @return the number of rows persisted
   */
  private int persistHydrant(FireHydrant indexToPersist, SegmentIdentifier identifier)
  {
    synchronized (indexToPersist) {
      if (indexToPersist.hasSwapped()) {
        log.info(
            "Segment[%s], Hydrant[%s] already swapped. Ignoring request to persist.",
            identifier, indexToPersist
        );
        return 0;
      }

      log.info("Segment[%s], persisting Hydrant[%s]", identifier, indexToPersist);

      try {
        int numRows = indexToPersist.getIndex().size();

        final File persistDir = createPersistDirIfNeeded(identifier);
        final IndexSpec indexSpec = tuningConfig.getIndexSpec();

        final long start = System.currentTimeMillis();
        final File persistedFile = indexMerger.persist(
            indexToPersist.getIndex(),
            identifier.getInterval(),
            new File(persistDir, String.valueOf(indexToPersist.getCount())),
            indexSpec
        );

        indexToPersist.persisted(
            indexIO.loadIndex(persistedFile),
            persistedFile,
            System.currentTimeMillis() - start
        );
        return numRows;
      }
      catch (IOException e) {
        log.makeAlert("dataSource[%s] -- incremental persist failed", schema.getDataSource())
           .addData("segment", identifier.getIdentifierAsString())
           .addData("count", indexToPersist.getCount())
           .emit();

        throw Throwables.propagate(e);
      }
    }
  }

  private void removeDirectory(final File target)
  {
    if (target.exists()) {
      try {
        log.info("Deleting Index File[%s]", target);
        FileUtils.deleteDirectory(target);
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to remove directory[%s]", schema.getDataSource())
           .addData("file", target)
           .emit();
      }
    }
  }

  private static String makeHydrantCacheIdentifier(FireHydrant input, Segment segment)
  {
    return segment.getIdentifier() + "_" + input.getCount();
  }

  public List<Sink> getSinks()
  {
    return ImmutableList.<Sink>copyOf(sinks.values());
  }
}
