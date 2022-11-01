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

package io.druid.segment.realtime.plumber;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.druid.cache.Cache;
import io.druid.client.CachingQueryRunner;
import io.druid.client.cache.CacheConfig;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.common.guava.ThreadRenamingRunnable;
import io.druid.common.utils.VMUtils;
import io.druid.concurrent.Execs;
import io.druid.concurrent.TaskThreadPriority;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.query.BySegmentQueryRunner;
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
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.Metadata;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 */
public class RealtimePlumber implements Plumber
{
  private static final EmittingLogger log = new EmittingLogger(RealtimePlumber.class);
  private static final int WARN_DELAY = 1000;
  private static final int MAX_ROW_EXCEED_CHECK_INTERVAL = 100000;

  private final DataSchema schema;
  private final RealtimeTuningConfig config;
  private final RejectionPolicy rejectionPolicy;
  private final FireDepartmentMetrics metrics;
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final ExecutorService queryExecutorService;
  private final DataSegmentPusher dataSegmentPusher;
  private final SegmentPublisher segmentPublisher;
  private final SegmentHandoffNotifier handoffNotifier;
  private final Object handoffCondition = new Object();
  private final Map<Long, Sink> sinks = Maps.newConcurrentMap();
  private final VersionedIntervalTimeline<Sink> sinkTimeline = new VersionedIntervalTimeline<Sink>();

  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final ObjectMapper objectMapper;

  private final int maxRowExceedCheckInterval;

  private volatile long nextFlush = 0;
  private volatile boolean shuttingDown = false;
  private volatile boolean stopped = false;
  private volatile boolean cleanShutdown = true;
  private volatile ExecutorService persistExecutor = null;
  private volatile ExecutorService mergeExecutor = null;
  private volatile ScheduledExecutorService scheduledExecutor = null;
  private final IndexMerger indexMerger;
  private final IndexIO indexIO;

  private static final String COMMIT_METADATA_KEY = "%commitMetadata%";
  private static final String COMMIT_METADATA_TIMESTAMP_KEY = "%commitMetadataTimestamp%";
  private static final String SKIP_INCREMENTAL_SEGMENT = "skipIncrementalSegment";

  private int counter;

  public RealtimePlumber(
      DataSchema schema,
      RealtimeTuningConfig config,
      FireDepartmentMetrics metrics,
      ServiceEmitter emitter,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ExecutorService queryExecutorService,
      DataSegmentPusher dataSegmentPusher,
      SegmentPublisher segmentPublisher,
      SegmentHandoffNotifier handoffNotifier,
      IndexMerger indexMerger,
      IndexIO indexIO,
      Cache cache,
      CacheConfig cacheConfig,
      ObjectMapper objectMapper
  )
  {
    this.schema = schema;
    this.config = config;
    this.rejectionPolicy = config.getRejectionPolicyFactory().create(config.getWindowPeriod());
    this.metrics = metrics;
    this.emitter = emitter;
    this.conglomerate = conglomerate;
    this.segmentAnnouncer = segmentAnnouncer;
    this.queryExecutorService = queryExecutorService;
    this.dataSegmentPusher = dataSegmentPusher;
    this.segmentPublisher = segmentPublisher;
    this.handoffNotifier = handoffNotifier;
    this.indexMerger = Preconditions.checkNotNull(indexMerger, "Null IndexMerger");
    this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.objectMapper = objectMapper;
    this.maxRowExceedCheckInterval = Math.min(config.getMaxRowsInMemory(), MAX_ROW_EXCEED_CHECK_INTERVAL);

    if (!cache.isLocal()) {
      log.error("Configured cache is not local, caching will not be enabled");
    }

    log.info("Creating plumber using rejectionPolicy[%s]", getRejectionPolicy());
  }

  public DataSchema getSchema()
  {
    return schema;
  }

  public RealtimeTuningConfig getConfig()
  {
    return config;
  }

  public RejectionPolicy getRejectionPolicy()
  {
    return rejectionPolicy;
  }

  public Map<Long, Sink> getSinks()
  {
    return sinks;
  }

  @Override
  public Object startJob()
  {
    computeBaseDir(schema).mkdirs();
    initializeExecutors();
    handoffNotifier.start();
    Object retVal = bootstrapSinksFromDisk();
    startPersistThread();
    // Push pending sinks bootstrapped from previous run
    mergeAndPush();
    resetNextFlush();
    return retVal;
  }

  @Override
  public int add(InputRow row, Supplier<Committer> committerSupplier) throws IndexSizeExceededException
  {
    final Sink sink = getSink(row.getTimestampFromEpoch());
    if (sink == null) {
      return -1;
    }

    final int numRows = sink.add(row);

    if (!sink.canAppendRow()) {
      log.info("Start flushing current sink.. %s", sink.getInterval());
      persist(committerSupplier.get(), sink);
    } else if (System.currentTimeMillis() > nextFlush) {
      log.info("Start flushing all sinks");
      persist(committerSupplier.get());
    }

    if (maxRowExceedCheckInterval > 0 && ++counter % maxRowExceedCheckInterval == 0) {
      if (rowCountInMemory() > config.getMaxRowsInMemory()) {
        log.info("Start flushing for exceeding max rows %,d/%,d", rowCountInMemory(), config.getMaxRowsInMemory());
        persist(committerSupplier.get(), findBiggest());
      } else if (config.getMaxOccupationInMemory() > 0 && occupationInMemory() > config.getMaxOccupationInMemory()) {
        log.info(
            "Start flushing for exceeding max occupation %,d/%,d",
            occupationInMemory(), config.getMaxOccupationInMemory()
        );
        persist(committerSupplier.get(), findBiggest());
      }
    }

    return numRows;
  }

  private int rowCountInMemory()
  {
    int size = 0;
    for (Sink aSink : sinks.values()) {
      size += aSink.rowCountInMemory();
    }
    return size;
  }

  private long occupationInMemory()
  {
    long size = 0;
    for (Sink aSink : sinks.values()) {
      size += aSink.occupationInMemory();
    }
    return size;
  }

  private Sink getSink(long timestamp)
  {
    if (!rejectionPolicy.accept(timestamp)) {
      return null;
    }

    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final VersioningPolicy versioningPolicy = config.getVersioningPolicy();

    final long truncatedTime = segmentGranularity.bucketStart(new DateTime(timestamp)).getMillis();

    Sink retVal = sinks.get(truncatedTime);

    if (retVal == null) {
      final Interval sinkInterval = new Interval(
          new DateTime(truncatedTime),
          segmentGranularity.increment(new DateTime(truncatedTime))
      );

      retVal = new Sink(
          sinkInterval,
          schema,
          config.getShardSpec(),
          versioningPolicy.getVersion(sinkInterval),
          config,
          ImmutableList.<FireHydrant>of(),
          objectMapper
      );
      addSink(retVal);

    }

    return retVal;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(final Query<T> query)
  {
    if (query instanceof Query.ManagementQuery) {
      return QueryRunnerHelper.toManagementRunner(query, conglomerate, null, objectMapper);
    }
    final boolean skipIncrementalSegment = query.getContextBoolean(SKIP_INCREMENTAL_SEGMENT, false);
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    final QueryToolChest<T, Query<T>> toolchest = factory.getToolchest();

    List<TimelineObjectHolder<Sink>> querySinks = Lists.newArrayList();
    for (Interval interval : query.getIntervals()) {
      querySinks.addAll(sinkTimeline.lookup(interval));
    }

    Iterable<QueryRunner<T>> runners = Iterables.transform(
        querySinks,
        holder -> {
          if (holder == null) {
            throw new ISE("No timeline entry at all!");
          }

          // The realtime plumber always uses SingleElementPartitionChunk
          final Sink theSink = holder.getObject().getChunk(0).getObject();

          if (theSink == null) {
            throw new ISE("Missing sink for timeline entry[%s]!", holder);
          }

          final SegmentDescriptor descriptor = new SegmentDescriptor(
              schema.getDataSource(),
              holder.getInterval(),
              theSink.getVersion(),
              theSink.getPartitionNum()
          );

          return new SpecificSegmentQueryRunner<T>(
              new MetricsEmittingQueryRunner<T>(
                  emitter,
                  toolchest,
                  factory.mergeRunners(
                      query,
                      Execs.newDirectExecutorService(),
                      Iterables.transform(
                          theSink,
                          hydrant -> {
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
                                  factory.createRunner(segment.lhs, null),
                                  segment.rhs
                              );

                              if (hydrantDefinitelySwapped // only use caching if data is immutable
                                  && cache.isLocal() // hydrants may not be in sync between replicas, make sure cache is local
                              ) {
                                baseRunner = new CachingQueryRunner<>(
                                    makeHydrantIdentifier(hydrant, segment.lhs),
                                    descriptor,
                                    objectMapper,
                                    cache,
                                    toolchest,
                                    baseRunner,
                                    Execs.newDirectExecutorService(),
                                    cacheConfig
                                );
                              }
                              return new BySegmentQueryRunner<T>(
                                  toolchest,
                                  segment.lhs.getIdentifier(),
                                  segment.lhs.getInterval().getStart(),
                                  baseRunner
                              );
                            }
                            catch (RuntimeException e) {
                              CloseQuietly.close(segment.rhs);
                              throw e;
                            }
                          }
                      ),
                      null
                  ),
                  QueryMetrics::reportSegmentAndCacheTime,
                  queryMetrics -> queryMetrics.segment(theSink.getIdentifier())
              ).withWaitMeasuredFromNow(),
              new SpecificSegmentSpec(
                  descriptor
              )
          );
        }
    );
    return toolchest.mergeResults(
        factory.mergeRunners(
            query,
            queryExecutorService,
            runners,
            null
        )
    );
  }

  protected static String makeHydrantIdentifier(FireHydrant input, Segment segment)
  {
    return segment.getIdentifier() + "_" + input.getCount();
  }

  @Override
  public void persist(final Committer committer)
  {
    final List<Pair<FireHydrant, Interval>> indexesToPersist = Lists.newArrayList();
    for (Sink sink : sinks.values()) {
      FireHydrant hydrant = sink.swap();
      if (hydrant != null) {
        indexesToPersist.add(Pair.of(hydrant, sink.getInterval()));
      }
    }
    log.info("Persisting %d hydrants", indexesToPersist.size());
    persist(committer, indexesToPersist);
  }

  private Sink findBiggest()
  {
    Sink maxRowsSink = null;
    long maxRowsInMemory = -1;
    for (Sink sink : sinks.values()) {
      int rowsInMemory = sink.rowCountInMemory();
      if (rowsInMemory > 0 && (maxRowsInMemory < 0 || rowsInMemory > maxRowsInMemory)) {
        maxRowsInMemory = rowsInMemory;
        maxRowsSink = sink;
      }
    }
    return maxRowsSink;
  }

  private void persist(final Committer committer, final Sink sink)
  {
    FireHydrant hydrant = sink.swap();
    if (hydrant != null) {
      persist(committer, Arrays.asList(Pair.of(hydrant, sink.getInterval())));
    }
  }

  private void persist(final Committer committer, final List<Pair<FireHydrant, Interval>> indexesToPersist)
  {
    final Stopwatch runExecStopwatch = Stopwatch.createStarted();
    final Stopwatch persistStopwatch = Stopwatch.createStarted();

    final Map<String, Object> metadataElems = committer.getMetadata() == null ? null :
                                              ImmutableMap.of(
                                                  COMMIT_METADATA_KEY,
                                                  committer.getMetadata(),
                                                  COMMIT_METADATA_TIMESTAMP_KEY,
                                                  System.currentTimeMillis()
                                              );

    persistExecutor.execute(
        new ThreadRenamingRunnable(String.format("%s-incremental-persist", schema.getDataSource()))
        {
          @Override
          public void doRun()
          {
            /* Note:
            If plumber crashes after storing a subset of all the hydrants then we will lose data and next
            time we will start with the commitMetadata stored in those hydrants.
            option#1:
            maybe it makes sense to store the metadata outside the segments in a separate file. This is because the
            commit metadata isn't really associated with an individual segment-- it's associated with a set of segments
            that are persisted at the same time or maybe whole datasource. So storing it in the segments is asking for problems.
            Sort of like this:

            {
              "metadata" : {"foo": "bar"},
              "segments": [
                {"id": "datasource_2000_2001_2000_1", "hydrant": 10},
                {"id": "datasource_2001_2002_2001_1", "hydrant": 12},
              ]
            }
            When a realtime node crashes and starts back up, it would delete any hydrants numbered higher than the
            ones in the commit file.

            option#2
            We could also just include the set of segments for the same chunk of metadata in more metadata on each
            of the segments. we might also have to think about the hand-off in terms of the full set of segments being
            handed off instead of individual segments being handed off (that is, if one of the set succeeds in handing
            off and the others fail, the real-time would believe that it needs to re-ingest the data).
             */
            long persistThreadCpuTime = VMUtils.safeGetThreadCpuTime();
            try {
              for (Pair<FireHydrant, Interval> pair : indexesToPersist) {
                metrics.incrementRowOutputCount(
                    persistHydrant(
                        pair.lhs, schema, pair.rhs, metadataElems
                    )
                );
              }
              committer.run();
            }
            catch (Exception e) {
              metrics.incrementFailedPersists();
              throw e;
            }
            finally {
              metrics.incrementPersistCpuTime(VMUtils.safeGetThreadCpuTime() - persistThreadCpuTime);
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
  }

  // Submits persist-n-merge task for a Sink to the mergeExecutor
  private void persistAndMerge(final long truncatedTime, final Sink sink)
  {
    final String threadName = String.format(
        "%s-%s-persist-n-merge", schema.getDataSource(), new DateTime(truncatedTime)
    );
    mergeExecutor.execute(
        new ThreadRenamingRunnable(threadName)
        {
          final Interval interval = sink.getInterval();
          Stopwatch mergeStopwatch = null;

          @Override
          public void doRun()
          {
            try {
              // Bail out if this sink has been abandoned by a previously-executed task.
              if (sinks.get(truncatedTime) != sink) {
                log.info("Sink[%s] was abandoned, bailing out of persist-n-merge.", sink);
                return;
              }

              // Use a file to indicate that pushing has completed.
              final File persistDir = computePersistDir(schema, interval);
              final File mergedTarget = new File(persistDir, "merged");
              final File isPushedMarker = new File(persistDir, "isPushedMarker");

              if (!isPushedMarker.exists()) {
                removeSegment(sink, mergedTarget);
                if (mergedTarget.exists()) {
                  log.error("Merged target[%s] exists?!", mergedTarget);
                  return;
                }
              } else {
                log.info("Already pushed sink[%s]", sink);
                return;
              }

            /*
            Note: it the plumber crashes after persisting a subset of hydrants then might duplicate data as these
            hydrants will be read but older commitMetadata will be used. fixing this possibly needs structural
            changes to plumber.
             */
              for (FireHydrant hydrant : sink) {
                synchronized (hydrant) {
                  if (!hydrant.hasSwapped()) {
                    log.info("Hydrant[%s] hasn't swapped yet, swapping. Sink[%s]", hydrant, sink);
                    final int rowCount = persistHydrant(hydrant, schema, interval, null);
                    metrics.incrementRowOutputCount(rowCount);
                  }
                }
              }
              final long mergeThreadCpuTime = VMUtils.safeGetThreadCpuTime();
              mergeStopwatch = Stopwatch.createStarted();

              final File mergedFile;
              List<FireHydrant> hydrants = Lists.newArrayList(sink);
              if (hydrants.size() == 1) {
                mergedFile = Iterables.getOnlyElement(hydrants).getPersistedPath();
              } else {
                List<QueryableIndex> indexes = Lists.newArrayList();
                for (FireHydrant fireHydrant : hydrants) {
                  log.info("Adding hydrant[%s]", fireHydrant);
                  indexes.add(fireHydrant.getSegment().asQueryableIndex(false));
                }

                mergedFile = indexMerger.mergeQueryableIndex(
                    indexes,
                    schema.getGranularitySpec().isRollup(),
                    schema.getAggregators(),
                    mergedTarget,
                    config.getIndexSpec()
                );
              }

              // emit merge metrics before publishing segment
              metrics.incrementMergeCpuTime(VMUtils.safeGetThreadCpuTime() - mergeThreadCpuTime);
              metrics.incrementMergeTimeMillis(mergeStopwatch.elapsed(TimeUnit.MILLISECONDS));

              final DataSegment template = indexIO.decorateMeta(sink.getSegment(), mergedFile);
              log.info("Pushing [%s] to deep storage", sink.getIdentifier());

              final DataSegment segment = dataSegmentPusher.push(mergedFile, template);
              log.info("Inserting [%s] to the metadata store", sink.getIdentifier());
              segmentPublisher.publishSegment(segment);

              if (!isPushedMarker.createNewFile()) {
                log.makeAlert("Failed to create marker file for [%s]", schema.getDataSource())
                   .addData("interval", sink.getInterval())
                   .addData("partitionNum", segment.getShardSpecWithDefault().getPartitionNum())
                   .addData("marker", isPushedMarker)
                   .emit();
              }
            }
            catch (Exception e) {
              metrics.incrementFailedHandoffs();
              log.makeAlert(e, "Failed to persist merged index[%s]", schema.getDataSource())
                 .addData("interval", interval)
                 .emit();
              if (shuttingDown) {
                // We're trying to shut down, and this segment failed to push. Let's just get rid of it.
                // This call will also delete possibly-partially-written files, so we don't need to do it explicitly.
                cleanShutdown = false;
                abandonSegment(truncatedTime, sink);
              }
            }
            finally {
              if (mergeStopwatch != null) {
                mergeStopwatch.stop();
              }
            }
          }
        }
    );
    handoffNotifier.registerSegmentHandoffCallback(
        new SegmentDescriptor(
            schema.getDataSource(),
            sink.getInterval(),
            sink.getVersion(),
            config.getShardSpec().getPartitionNum()
        ),
        mergeExecutor, new Runnable()
        {
          @Override
          public void run()
          {
            abandonSegment(sink.getInterval().getStartMillis(), sink);
            metrics.incrementHandOffCount();
          }
        }
    );
  }

  @Override
  public void finishJob()
  {
    log.info("Shutting down...");

    shuttingDown = true;

    for (final Map.Entry<Long, Sink> entry : sinks.entrySet()) {
      persistAndMerge(entry.getKey(), entry.getValue());
    }

    final long forceEndWaitTime = System.currentTimeMillis() + config.getHandoffConditionTimeout();
    while (!sinks.isEmpty()) {
      try {
        log.info(
            "Waiting sinks handed-off.. remaining: %s",
            Joiner.on(", ").join(
                Iterables.transform(
                    sinks.values(),
                    Sink::getIdentifier
                )
            )
        );

        synchronized (handoffCondition) {
          while (!sinks.isEmpty()) {
            if (config.getHandoffConditionTimeout() == 0) {
              handoffCondition.wait();
            } else {
              long curr = System.currentTimeMillis();
              if (forceEndWaitTime - curr > 0) {
                handoffCondition.wait(forceEndWaitTime - curr);
              } else {
                throw new ISE(
                    "Segment handoff wait timeout. [%s] segments might not have completed handoff.",
                    sinks.size()
                );
              }
            }
          }
        }
      }
      catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    handoffNotifier.close();
    shutdownExecutors();

    stopped = true;

    if (!cleanShutdown) {
      throw new ISE("Exception occurred during persist and merge.");
    }
  }

  private void resetNextFlush()
  {
    nextFlush = new DateTime().plus(config.getIntermediatePersistPeriod()).getMillis();
  }

  protected void initializeExecutors()
  {
    final int maxPendingPersists = config.getMaxPendingPersists();

    if (persistExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      persistExecutor = Execs.newBlockingSingleThreaded(
          "plumber_persist_%d",
          maxPendingPersists,
          TaskThreadPriority.getThreadPriorityFromTaskPriority(config.getPersistThreadPriority())
      );
    }
    if (mergeExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      mergeExecutor = Execs.newBlockingSingleThreaded(
          "plumber_merge_%d",
          1,
          TaskThreadPriority.getThreadPriorityFromTaskPriority(config.getMergeThreadPriority())
      );
    }

    if (scheduledExecutor == null) {
      scheduledExecutor = Execs.scheduledSingleThreaded("plumber_scheduled_%d");
    }
  }

  protected void shutdownExecutors()
  {
    // scheduledExecutor is shutdown here
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
      persistExecutor.shutdown();
      mergeExecutor.shutdown();
    }
  }

  protected Object bootstrapSinksFromDisk()
  {
    final VersioningPolicy versioningPolicy = config.getVersioningPolicy();

    File baseDir = computeBaseDir(schema);
    if (baseDir == null || !baseDir.exists()) {
      return null;
    }

    File[] files = baseDir.listFiles();
    if (files == null) {
      return null;
    }

    if (config.isIgnorePreviousSegments()) {
      for (File file : files) {
        FileUtils.deleteQuietly(file);
      }
      return null;
    }

    Object metadata = null;
    long latestCommitTime = 0;
    for (File sinkDir : files) {
      final Interval sinkInterval = new Interval(sinkDir.getName().replace("_", "/"));

      //final File[] sinkFiles = sinkDir.listFiles();
      // To avoid reading and listing of "merged" dir
      final File[] sinkFiles = sinkDir.listFiles((dir, fileName) -> Ints.tryParse(fileName) != null);
      Arrays.sort(
          sinkFiles,
          new Comparator<File>()
          {
            @Override
            public int compare(File o1, File o2)
            {
              try {
                return Integer.compare(Integer.parseInt(o1.getName()), Integer.parseInt(o2.getName()));
              }
              catch (NumberFormatException e) {
                log.error(e, "Couldn't compare as numbers? [%s][%s]", o1, o2);
                return o1.compareTo(o2);
              }
            }
          }
      );
      final ShardSpec shardSpec = config.getShardSpec();
      final DataSegment template = new DataSegment(
          schema.getDataSource(),
          sinkInterval,
          versioningPolicy.getVersion(sinkInterval),
          ImmutableMap.of(),
          ImmutableList.of(),
          ImmutableList.of(),
          shardSpec,
          null,
          0
      );

      boolean isCorrupted = false;
      List<FireHydrant> hydrants = Lists.newArrayList();
      for (File segmentDir : sinkFiles) {
        log.info("Loading previously persisted segment at [%s]", segmentDir);

        // Although this has been tackled at start of this method.
        // Just a doubly-check added to skip "merged" dir. from being added to hydrants
        // If 100% sure that this is not needed, this check can be removed.
        if (Ints.tryParse(segmentDir.getName()) == null) {
          continue;
        }
        QueryableIndex index = null;
        try {
          index = indexIO.loadIndex(segmentDir);
        }
        catch (IOException e) {
          log.error(e, "Problem loading segmentDir from disk.");
          isCorrupted = true;
        }
        if (isCorrupted) {
          try {
            File corruptSegmentDir = computeCorruptedFileDumpDir(segmentDir, schema);
            log.info("Renaming %s to %s", segmentDir.getAbsolutePath(), corruptSegmentDir.getAbsolutePath());
            FileUtils.copyDirectory(segmentDir, corruptSegmentDir);
            FileUtils.deleteDirectory(segmentDir);
          }
          catch (Exception e1) {
            log.error(e1, "Failed to rename %s", segmentDir.getAbsolutePath());
          }
          //Note: skipping corrupted segment might lead to dropping some data. This strategy should be changed
          //at some point.
          continue;
        }
        Metadata segmentMetadata = index.getMetadata();
        if (segmentMetadata != null) {
          Object timestampObj = segmentMetadata.get(COMMIT_METADATA_TIMESTAMP_KEY);
          if (timestampObj != null) {
            long timestamp = ((Long) timestampObj).longValue();
            if (timestamp > latestCommitTime) {
              log.info(
                  "Found metaData [%s] with latestCommitTime [%s] greater than previous recorded [%s]",
                  index.getMetadata(), timestamp, latestCommitTime
              );
              latestCommitTime = timestamp;
              metadata = index.getMetadata().get(COMMIT_METADATA_KEY);
            }
          }
        }
        DataSegment segment = template.withDimensions(Lists.newArrayList(index.getAvailableDimensions()))
                                      .withMetrics(Lists.newArrayList(index.getAvailableMetrics()))
                                      .withNumRows(index.getNumRows());
        hydrants.add(
            new FireHydrant(
                new QueryableIndexSegment(index, segment),
                segmentDir,
                Integer.parseInt(segmentDir.getName())
            )
        );
      }
      if (hydrants.isEmpty()) {
        // Probably encountered a corrupt sink directory
        log.warn(
            "Found persisted segment directory with no intermediate segments present at %s, skipping sink creation.",
            sinkDir.getAbsolutePath()
        );
        continue;
      }
      final Sink currSink = new Sink(
          sinkInterval,
          schema,
          shardSpec,
          versioningPolicy.getVersion(sinkInterval),
          config,
          hydrants,
          objectMapper
      );
      addSink(currSink);
    }
    return metadata;
  }

  private void addSink(final Sink sink)
  {
    sinks.put(sink.getInterval().getStartMillis(), sink);
    sinkTimeline.add(
        sink.getInterval(),
        sink.getVersion(),
        new SingleElementPartitionChunk<Sink>(sink)
    );
    try {
      segmentAnnouncer.announceSegment(sink.getSegment());
    }
    catch (IOException e) {
      log.makeAlert(e, "Failed to announce new segment[%s]", schema.getDataSource())
         .addData("interval", sink.getInterval())
         .emit();
    }
  }

  protected void startPersistThread()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final DateTime truncatedNow = segmentGranularity.bucketStart(new DateTime());
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    log.info(
        "Expect to run at [%s]",
        new DateTime().plus(
            new Duration(
                System.currentTimeMillis(),
                segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
            )
        )
    );

    ScheduledExecutors
        .scheduleAtFixedRate(
            scheduledExecutor,
            new Duration(
                System.currentTimeMillis(),
                segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
            ),
            new Duration(truncatedNow, segmentGranularity.increment(truncatedNow)),
            new ThreadRenamingCallable<ScheduledExecutors.Signal>(
                String.format(
                    "%s-overseer-%d",
                    schema.getDataSource(),
                    config.getShardSpec().getPartitionNum()
                )
            )
            {
              @Override
              public ScheduledExecutors.Signal doCall()
              {
                if (stopped) {
                  log.info("Stopping merge-n-push overseer thread");
                  return ScheduledExecutors.Signal.STOP;
                }

                mergeAndPush();

                if (stopped) {
                  log.info("Stopping merge-n-push overseer thread");
                  return ScheduledExecutors.Signal.STOP;
                } else {
                  return ScheduledExecutors.Signal.REPEAT;
                }
              }
            }
        );
  }

  private void mergeAndPush()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final long windowMillis = windowPeriod.toStandardDuration().getMillis();
    log.info("Starting merge and push.");
    DateTime minTimestampAsDate = segmentGranularity.bucketStart(
        new DateTime(
            Math.max(
                windowMillis,
                rejectionPolicy.getCurrMaxTime()
                               .getMillis()
            )
            - windowMillis
        )
    );
    long minTimestamp = minTimestampAsDate.getMillis();

    log.info(
        "Found [%,d] segments. Attempting to hand off segments that start before [%s].",
        sinks.size(),
        minTimestampAsDate
    );

    List<Map.Entry<Long, Sink>> sinksToPush = Lists.newArrayList();
    for (Map.Entry<Long, Sink> entry : sinks.entrySet()) {
      final Long intervalStart = entry.getKey();
      if (intervalStart < minTimestamp) {
        log.info("Adding entry [%s] for merge and push.", entry);
        sinksToPush.add(entry);
      } else {
        log.info(
            "Skipping persist and merge for entry [%s] : Start time [%s] >= [%s] min timestamp required in this run. Segment will be picked up in a future run.",
            entry,
            new DateTime(intervalStart),
            minTimestampAsDate
        );
      }
    }

    log.info("Found [%,d] sinks to persist and merge", sinksToPush.size());

    for (final Map.Entry<Long, Sink> entry : sinksToPush) {
      persistAndMerge(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Unannounces a given sink and removes all local references to it. It is important that this is only called
   * from the single-threaded mergeExecutor, since otherwise chaos may ensue if merged segments are deleted while
   * being created.
   *
   * @param truncatedTime sink key
   * @param sink          sink to unannounce
   */
  protected void abandonSegment(final long truncatedTime, final Sink sink)
  {
    if (sinks.containsKey(truncatedTime)) {
      try {
        segmentAnnouncer.unannounceSegment(sink.getSegment());
        removeSegment(sink, computePersistDir(schema, sink.getInterval()));
        log.info("Removing sinkKey %d for segment %s", truncatedTime, sink.getIdentifier());
        sinks.remove(truncatedTime);
        sinkTimeline.remove(
            sink.getInterval(),
            sink.getVersion(),
            new SingleElementPartitionChunk<>(sink)
        );
        for (FireHydrant hydrant : sink) {
          cache.close(makeHydrantIdentifier(hydrant, hydrant.getSegment()));
        }
        synchronized (handoffCondition) {
          handoffCondition.notifyAll();
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to abandon old segment for dataSource[%s]", schema.getDataSource())
           .addData("interval", sink.getInterval())
           .emit();
      }
    }
  }

  protected File computeBaseDir(DataSchema schema)
  {
    return new File(config.getBasePersistDirectory(), schema.getDataSource());
  }

  protected File computeCorruptedFileDumpDir(File persistDir, DataSchema schema)
  {
    return new File(
        persistDir.getAbsolutePath()
                  .replace(schema.getDataSource(), "corrupted" + File.pathSeparator + schema.getDataSource())
    );
  }

  protected File computePersistDir(DataSchema schema, Interval interval)
  {
    return new File(computeBaseDir(schema), interval.toString().replace("/", "_"));
  }

  /**
   * Persists the given hydrant and returns the number of rows persisted
   *
   * @param indexToPersist hydrant to persist
   * @param schema         datasource schema
   * @param interval       interval to persist
   *
   * @return the number of rows persisted
   */
  protected int persistHydrant(
      FireHydrant indexToPersist,
      DataSchema schema,
      Interval interval,
      Map<String, Object> metadataElems
  )
  {
    synchronized (indexToPersist) {
      if (indexToPersist.hasSwapped()) {
        log.info(
            "DataSource[%s], Interval[%s], Hydrant[%s] already swapped. Ignoring request to persist.",
            schema.getDataSource(), interval, indexToPersist
        );
        return 0;
      }

      log.info(
          "DataSource[%s], Interval[%s], Metadata [%s] persisting Hydrant[%s]",
          schema.getDataSource(),
          interval,
          metadataElems,
          indexToPersist
      );
      final IncrementalIndex index = indexToPersist.getIndex();
      try {
        int numRows = index.size();

        index.getMetadata().putAll(metadataElems);

        final long start = System.currentTimeMillis();
        final File persistedFile = indexMerger.persist(
            index,
            interval,
            new File(computePersistDir(schema, interval), String.valueOf(indexToPersist.getCount())),
            config.getIndexSpec()
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
           .addData("interval", interval)
           .addData("count", indexToPersist.getCount())
           .emit();

        throw Throwables.propagate(e);
      }
    }
  }

  private void removeSegment(final Sink sink, final File target)
  {
    if (target.exists()) {
      try {
        log.info("Deleting Index File[%s]", target);
        FileUtils.deleteDirectory(target);
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to remove file for dataSource[%s]", schema.getDataSource())
           .addData("file", target)
           .addData("interval", sink.getInterval())
           .emit();
      }
    }
  }
}
