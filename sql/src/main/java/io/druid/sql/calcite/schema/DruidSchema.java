/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.sql.calcite.schema;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.common.DateTimes;
import io.druid.common.Yielders;
import io.druid.data.ValueDesc;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import io.druid.client.ServerView;
import io.druid.client.TimelineServerView;
import io.druid.guice.ManageLifecycle;
import io.druid.query.TableDataSource;
import io.druid.query.metadata.metadata.AllColumnIncluderator;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.table.RowSignature;
import io.druid.sql.calcite.view.DruidViewMacro;
import io.druid.sql.calcite.view.ViewManager;
import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@ManageLifecycle
public class DruidSchema extends AbstractSchema
{
  // Newest segments first, so they override older ones.
  private static final Comparator<DataSegment> SEGMENT_ORDER = Comparator
      .comparing((DataSegment segment) -> segment.getInterval().getStart()).reversed()
      .thenComparing(Function.identity());

  public static final String NAME = "druid";

  private static final EmittingLogger log = new EmittingLogger(DruidSchema.class);
  private static final int MAX_SEGMENTS_PER_QUERY = 15000;

  private final QuerySegmentWalker segmentWalker;
  private final PlannerConfig config;
  private final ViewManager viewManager;
  private final ExecutorService cacheExec;
  private final ConcurrentMap<String, DruidTable> tables;

  // For awaitInitialization.
  private final CountDownLatch initializationLatch = new CountDownLatch(1);

  // Protects access to segmentSignatures, mutableSegments, segmentsNeedingRefresh, lastRefresh, isServerViewInitialized
  private final Object lock = new Object();

  // DataSource -> Segment -> SegmentMetadataHolder(contains RowSignature) for that segment.
  // Use TreeMap for segments so they are merged in deterministic order, from older to newer.
  // This data structure need to be accessed in a thread-safe way since SystemSchema accesses it
  private final Map<String, TreeMap<DataSegment, SegmentMetadataHolder>> segmentMetadataInfo = new HashMap<>();

  // All mutable segments.
  private final Set<DataSegment> mutableSegments = new TreeSet<>(SEGMENT_ORDER);

  // All dataSources that need tables regenerated.
  private final Set<String> dataSourcesNeedingRebuild = new HashSet<>();

  // All segments that need to be refreshed.
  private final TreeSet<DataSegment> segmentsNeedingRefresh = new TreeSet<>(SEGMENT_ORDER);

  private boolean refreshImmediately = false;
  private long lastRefresh = 0L;
  private long lastFailure = 0L;
  private boolean isServerViewInitialized = false;

  @Inject
  public DruidSchema(
      final @JacksonInject QuerySegmentWalker segmentWalker,
      final TimelineServerView serverView,
      final PlannerConfig config,
      final ViewManager viewManager
  )
  {
    this.segmentWalker = Preconditions.checkNotNull(segmentWalker, "segmentWalker");
    Preconditions.checkNotNull(serverView, "serverView");
    this.config = Preconditions.checkNotNull(config, "config");
    this.viewManager = Preconditions.checkNotNull(viewManager, "viewManager");
    this.cacheExec = ScheduledExecutors.fixed(1, "DruidSchema-Cache-%d");
    this.tables = new ConcurrentHashMap<>();

    serverView.registerTimelineCallback(
        MoreExecutors.sameThreadExecutor(),
        new TimelineServerView.TimelineCallback()
        {
          @Override
          public ServerView.CallbackAction timelineInitialized()
          {
            synchronized (lock) {
              isServerViewInitialized = true;
              lock.notifyAll();
            }

            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentAdded(final DruidServerMetadata server, final DataSegment segment)
          {
            addSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DruidServerMetadata server, final DataSegment segment)
          {
            removeSegment(segment);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  @LifecycleStart
  public void start()
  {
    cacheExec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              while (!Thread.currentThread().isInterrupted()) {
                final Set<DataSegment> segmentsToRefresh = new TreeSet<>();
                final Set<String> dataSourcesToRebuild = new TreeSet<>();

                try {
                  synchronized (lock) {
                    final long nextRefreshNoFuzz = DateTimes
                        .utc(lastRefresh)
                        .plus(config.getMetadataRefreshPeriod())
                        .getMillis();

                    // Fuzz a bit to spread load out when we have multiple brokers.
                    final long nextRefresh = nextRefreshNoFuzz + (long) ((nextRefreshNoFuzz - lastRefresh) * 0.10);

                    while (true) {
                      // Do not refresh if it's too soon after a failure (to avoid rapid cycles of failure).
                      final boolean wasRecentFailure = DateTimes.utc(lastFailure)
                                                                .plus(config.getMetadataRefreshPeriod())
                                                                .isAfterNow();

                      if (isServerViewInitialized &&
                          !wasRecentFailure &&
                          (!segmentsNeedingRefresh.isEmpty() || !dataSourcesNeedingRebuild.isEmpty()) &&
                          (refreshImmediately || nextRefresh < System.currentTimeMillis())) {
                        break;
                      }
                      lock.wait(Math.max(1, nextRefresh - System.currentTimeMillis()));
                    }

                    segmentsToRefresh.addAll(segmentsNeedingRefresh);
                    segmentsNeedingRefresh.clear();

                    // Mutable segments need a refresh every period, since new columns could be added dynamically.
                    segmentsNeedingRefresh.addAll(mutableSegments);

                    lastFailure = 0L;
                    lastRefresh = System.currentTimeMillis();
                    refreshImmediately = false;
                  }

                  // Refresh the segments.
                  final Set<DataSegment> refreshed = refreshSegments(segmentsToRefresh);

                  synchronized (lock) {
                    // Add missing segments back to the refresh list.
                    segmentsNeedingRefresh.addAll(Sets.difference(segmentsToRefresh, refreshed));

                    // Compute the list of dataSources to rebuild tables for.
                    dataSourcesToRebuild.addAll(dataSourcesNeedingRebuild);
                    refreshed.forEach(segment -> dataSourcesToRebuild.add(segment.getDataSource()));
                    dataSourcesNeedingRebuild.clear();

                    lock.notifyAll();
                  }

                  // Rebuild the dataSources.
                  for (String dataSource : dataSourcesToRebuild) {
                    final DruidTable druidTable = buildDruidTable(dataSource);
                    final DruidTable oldTable = tables.put(dataSource, druidTable);
                    if (oldTable == null || !oldTable.getRowSignature().equals(druidTable.getRowSignature())) {
                      log.debug(
                          "Table for dataSource[%s] has new signature[%s].",
                          dataSource,
                          druidTable.getRowSignature()
                      );
                    } else {
                      log.debug("Table for dataSource[%s] signature is unchanged.", dataSource);
                    }
                  }

                  initializationLatch.countDown();
                }
                catch (InterruptedException e) {
                  // Fall through.
                  throw e;
                }
                catch (Exception e) {
                  log.warn(e, "Metadata refresh failed, trying again soon.");

                  synchronized (lock) {
                    // Add our segments and dataSources back to their refresh and rebuild lists.
                    segmentsNeedingRefresh.addAll(segmentsToRefresh);
                    dataSourcesNeedingRebuild.addAll(dataSourcesToRebuild);
                    lastFailure = System.currentTimeMillis();
                    lock.notifyAll();
                  }
                }
              }
            }
            catch (InterruptedException e) {
              // Just exit.
            }
            catch (Throwable e) {
              // Throwables that fall out to here (not caught by an inner try/catch) are potentially gnarly, like
              // OOMEs. Anyway, let's just emit an alert and stop refreshing metadata.
              log.makeAlert(e, "Metadata refresh failed permanently").emit();
              throw e;
            }
            finally {
              log.info("Metadata refresh stopped.");
            }
          }
        }
    );
  }

  @LifecycleStop
  public void stop()
  {
    cacheExec.shutdownNow();
  }

  @VisibleForTesting
  public void awaitInitialization() throws InterruptedException
  {
    initializationLatch.await();
  }

  @Override
  protected Map<String, Table> getTableMap()
  {
    return ImmutableMap.copyOf(tables);
  }

  @Override
  protected Multimap<String, org.apache.calcite.schema.Function> getFunctionMultimap()
  {
    final ImmutableMultimap.Builder<String, org.apache.calcite.schema.Function> builder = ImmutableMultimap.builder();
    for (Map.Entry<String, DruidViewMacro> entry : viewManager.getViews().entrySet()) {
      builder.put(entry);
    }
    return builder.build();
  }

  private void addSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    synchronized (lock) {
      final Map<DataSegment, SegmentMetadataHolder> knownSegments = segmentMetadataInfo.get(segment.getDataSource());
      if (knownSegments == null || !knownSegments.containsKey(segment)) {
        // segmentReplicatable is used to determine if segments are served by realtime servers or not
        final long isRealtime = server.isAssignable() ? 0 : 1;
        final long isPublished = server.isHistorical() ? 1 : 0;
        final SegmentMetadataHolder holder = new SegmentMetadataHolder(
            segment.getIdentifier(), null, 0, isPublished, 1, isRealtime, 1
        );
        // Unknown segment.
        setSegmentSignature(segment, holder);
        segmentsNeedingRefresh.add(segment);
        if (!server.isAssignable()) {
          log.debug("Added new mutable segment[%s].", segment.getIdentifier());
          mutableSegments.add(segment);
        } else {
          log.debug("Added new immutable segment[%s].", segment.getIdentifier());
        }
      } else {
        if (knownSegments.containsKey(segment)) {
          final SegmentMetadataHolder holder = knownSegments.get(segment);
          final SegmentMetadataHolder holderWithNumReplicas = holder.withNumReplicas(holder.getNumReplicas() + 1);
          knownSegments.put(segment, holderWithNumReplicas);
        }
        if (server.isAssignable()) {
          // If a segment shows up on a replicatable (historical) server at any point, then it must be immutable,
          // even if it's also available on non-replicatable (realtime) servers.
          mutableSegments.remove(segment);
          log.debug("Segment[%s] has become immutable.", segment.getIdentifier());
        }
      }
      if (!tables.containsKey(segment.getDataSource())) {
        refreshImmediately = true;
      }

      lock.notifyAll();
    }
  }

  private void removeSegment(final DataSegment segment)
  {
    synchronized (lock) {
      log.debug("Segment[%s] is gone.", segment.getIdentifier());

      dataSourcesNeedingRebuild.add(segment.getDataSource());
      segmentsNeedingRefresh.remove(segment);
      mutableSegments.remove(segment);

      final Map<DataSegment, SegmentMetadataHolder> dataSourceSegments = segmentMetadataInfo.get(segment.getDataSource());
      dataSourceSegments.remove(segment);

      if (dataSourceSegments.isEmpty()) {
        segmentMetadataInfo.remove(segment.getDataSource());
        tables.remove(segment.getDataSource());
        log.info("Removed all metadata for dataSource[%s].", segment.getDataSource());
      }

      lock.notifyAll();
    }
  }

  /**
   * Attempt to refresh "segmentSignatures" for a set of segments. Returns the set of segments actually refreshed,
   * which may be a subset of the asked-for set.
   */
  private Set<DataSegment> refreshSegments(final Set<DataSegment> segments) throws IOException
  {
    final Set<DataSegment> retVal = new HashSet<>();

    // Organize segments by dataSource.
    final Map<String, TreeSet<DataSegment>> segmentMap = new TreeMap<>();

    for (DataSegment segment : segments) {
      segmentMap.computeIfAbsent(segment.getDataSource(), x -> new TreeSet<>(SEGMENT_ORDER))
                .add(segment);
    }

    for (Map.Entry<String, TreeSet<DataSegment>> entry : segmentMap.entrySet()) {
      final String dataSource = entry.getKey();
      retVal.addAll(refreshSegmentsForDataSource(dataSource, entry.getValue()));
    }

    return retVal;
  }

  /**
   * Attempt to refresh "segmentSignatures" for a set of segments for a particular dataSource. Returns the set of
   * segments actually refreshed, which may be a subset of the asked-for set.
   */
  private Set<DataSegment> refreshSegmentsForDataSource(
      final String dataSource,
      final Set<DataSegment> segments
  ) throws IOException
  {
    log.debug("Refreshing metadata for dataSource[%s].", dataSource);

    final long startTime = System.currentTimeMillis();

    // Segment identifier -> segment object.
    final Map<String, DataSegment> segmentMap = segments.stream().collect(
        Collectors.toMap(
            DataSegment::getIdentifier,
            Function.identity()
        )
    );

    final Set<DataSegment> retVal = new HashSet<>();
    final Sequence<SegmentAnalysis> sequence = runSegmentMetadataQuery(
        segmentWalker, Iterables.limit(segments, MAX_SEGMENTS_PER_QUERY)
    );

    Yielder<SegmentAnalysis> yielder = Yielders.each(sequence);

    try {
      while (!yielder.isDone()) {
        final SegmentAnalysis analysis = yielder.get();
        final DataSegment segment = segmentMap.get(analysis.getId());

        if (segment == null) {
          log.debug("Got analysis for segment[%s] we didn't ask for, ignoring.", analysis.getId());
        } else {
          final RowSignature rowSignature = analysisToRowSignature(analysis);
          log.debug("Segment[%s] has signature[%s].", segment.getIdentifier(), rowSignature);
          synchronized (lock) {
            Map<DataSegment, SegmentMetadataHolder> dataSourceSegments = segmentMetadataInfo.get(segment.getDataSource());
            SegmentMetadataHolder holder = dataSourceSegments.get(segment);
            SegmentMetadataHolder updatedHolder = holder.withRowSignature(rowSignature)
                                                        .withNumRows(analysis.getNumRows());
            dataSourceSegments.put(segment, updatedHolder);
            setSegmentSignature(segment, updatedHolder);
            retVal.add(segment);
          }
        }

        yielder = yielder.next(null);
      }
    }
    finally {
      yielder.close();
    }

    log.debug(
        "Refreshed metadata for dataSource[%s] in %,d ms (%d segments queried, %d segments left).",
        dataSource,
        System.currentTimeMillis() - startTime,
        retVal.size(),
        segments.size() - retVal.size()
    );

    return retVal;
  }

  private void setSegmentSignature(final DataSegment segment, final SegmentMetadataHolder segmentMetadataHolder)
  {
    synchronized (lock) {
      segmentMetadataInfo.computeIfAbsent(segment.getDataSource(), x -> new TreeMap<>(SEGMENT_ORDER))
                         .put(segment, segmentMetadataHolder);
    }
  }

  private DruidTable buildDruidTable(final String dataSource)
  {
    synchronized (lock) {
      final TreeMap<DataSegment, SegmentMetadataHolder> segmentMap = segmentMetadataInfo.get(dataSource);
      final Map<String, ValueDesc> columnTypes = new TreeMap<>();

      int totalNumRows = 0;
      if (segmentMap != null) {
        for (SegmentMetadataHolder segmentMetadataHolder : segmentMap.values()) {
          final RowSignature rowSignature = segmentMetadataHolder.getRowSignature();
          if (rowSignature != null) {
            for (String column : rowSignature.getRowOrder()) {
              // Newer column types should override older ones.
              columnTypes.putIfAbsent(column, rowSignature.getColumnType(column));
            }
          }
          totalNumRows += segmentMetadataHolder.getNumRows();
        }
      }

      final RowSignature.Builder builder = RowSignature.builder();
      columnTypes.forEach(builder::add);
      return new DruidTable(TableDataSource.of(dataSource), builder.build(), totalNumRows);
    }
  }

  private static Sequence<SegmentAnalysis> runSegmentMetadataQuery(
      final QuerySegmentWalker segmentWalker,
      final Iterable<DataSegment> segments
  )
  {
    // Sanity check: getOnlyElement of a set, to ensure all segments have the same dataSource.
    final String dataSource = Iterables.getOnlyElement(
        StreamSupport.stream(segments.spliterator(), false)
                     .map(DataSegment::getDataSource).collect(Collectors.toSet())
    );

    final MultipleSpecificSegmentSpec querySegmentSpec = new MultipleSpecificSegmentSpec(
        StreamSupport.stream(segments.spliterator(), false)
                     .map(DataSegment::toDescriptor).collect(Collectors.toList())
    );

    final Query<SegmentAnalysis> segmentMetadataQuery = new SegmentMetadataQuery(
        new TableDataSource(dataSource),
        querySegmentSpec,
        null,
        new AllColumnIncluderator(),
        null,
        false,
        ImmutableMap.of(),
        EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        false
    ).withId(UUID.randomUUID().toString());

    return segmentMetadataQuery.run(segmentWalker, Maps.newHashMap());
  }

  private static RowSignature analysisToRowSignature(final SegmentAnalysis analysis)
  {
    final RowSignature.Builder rowSignatureBuilder = RowSignature.builder();

    for (Map.Entry<String, ColumnAnalysis> entry : analysis.getColumns().entrySet()) {
      if (entry.getValue().isError()) {
        // Skip columns with analysis errors.
        continue;
      }

      ValueDesc valueDesc;
      try {
        valueDesc = ValueDesc.of(entry.getValue().getType());
      }
      catch (IllegalArgumentException e) {
        // Assume unrecognized types are some flavor of COMPLEX. This throws away information about exactly
        // what kind of complex column it is, which we may want to preserve some day.
        valueDesc = ValueDesc.UNKNOWN;
      }

      rowSignatureBuilder.add(entry.getKey(), valueDesc);
    }

    return rowSignatureBuilder.build();
  }

  public Map<DataSegment, SegmentMetadataHolder> getSegmentMetadata()
  {
    final Map<DataSegment, SegmentMetadataHolder> segmentMetadata = new HashMap<>();
    synchronized (lock) {
      for (TreeMap<DataSegment, SegmentMetadataHolder> val : segmentMetadataInfo.values()) {
        segmentMetadata.putAll(val);
      }
    }
    return segmentMetadata;
  }
}
