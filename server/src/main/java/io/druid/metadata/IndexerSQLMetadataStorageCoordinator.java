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

package io.druid.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.inject.Inject;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.NoopEmitter;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.server.log.Events;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class IndexerSQLMetadataStorageCoordinator implements IndexerMetadataStorageCoordinator
{
  private static final Logger log = new Logger(IndexerSQLMetadataStorageCoordinator.class);

  private final ObjectMapper jsonMapper;
  private final MetadataStorageTablesConfig dbTables;
  private final SQLMetadataConnector connector;
  private final Emitter emitter;

  private final String selectId;
  private final String insertSegment;
  private final String selectPayload;
  private final String updatePayload;
  private final String deleteSegment;

  private final String insertPending;
  private final String selectPending;
  private final String selectPendingOnInterval;
  private final String selectPendingsOnInterval;
  private final String deletePendings;

  private final String insertMetadata;
  private final String updateMetadata;
  private final String deleteMetadata;
  private final String resetMetadata;

  @Inject
  public IndexerSQLMetadataStorageCoordinator(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      @Events Emitter emitter
  )
  {
    this.jsonMapper = jsonMapper;
    this.dbTables = dbTables;
    this.connector = connector;
    this.emitter = emitter;

    final String quoteString = connector.getQuoteString();
    final String segmentsTable = dbTables.getSegmentsTable();
    this.selectId = String.format("SELECT id FROM %s WHERE id = :identifier", segmentsTable);
    this.insertSegment = String.format(
        "INSERT INTO %s (id, dataSource, created_date, start, \"end\", partitioned, version, used, payload) "
        + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
        segmentsTable
    );
    this.selectPayload = String.format(
        "SELECT payload FROM %s WHERE dataSource = :dataSource and start >= :start and \"end\" <= :end and used = false",
        segmentsTable
    );
    this.updatePayload = String.format("UPDATE %s SET payload = :payload WHERE id = :id", segmentsTable);
    this.deleteSegment = String.format("DELETE from %s WHERE id = :id", segmentsTable);

    final String pendingsTable = dbTables.getPendingSegmentsTable();
    this.insertPending = StringUtils.format(
        "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, sequence_name, sequence_prev_id, sequence_name_prev_id_sha1, payload) "
        + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, :sequence_name_prev_id_sha1, :payload)",
        pendingsTable,
        quoteString
    );
    this.selectPending = StringUtils.format(
        "SELECT payload FROM %s WHERE dataSource = :dataSource AND "
        + "sequence_name = :sequence_name AND "
        + "sequence_prev_id = :sequence_prev_id",
        pendingsTable
    );
    this.selectPendingOnInterval = StringUtils.format(
        "SELECT payload FROM %s WHERE "
        + "dataSource = :dataSource AND "
        + "sequence_name = :sequence_name AND "
        + "start = :start AND "
        + "%2$send%2$s = :end",
        pendingsTable,
        quoteString
    );
    this.selectPendingsOnInterval = String.format(
        "SELECT payload FROM %s WHERE dataSource = :dataSource AND start <= :end and \"end\" >= :start",
        pendingsTable
    );
    this.deletePendings = StringUtils.format(
        "delete from %s where datasource = :dataSource and created_date >= :start and created_date < :end",
        pendingsTable
    );

    final String dataSourceTable = dbTables.getDataSourceTable();
    this.insertMetadata = String.format(
        "INSERT INTO %s (dataSource, created_date, commit_metadata_payload, commit_metadata_sha1) "
        + "VALUES (:dataSource, :created_date, :commit_metadata_payload, :commit_metadata_sha1)",
        dataSourceTable
    );
    this.updateMetadata = String.format(
        "UPDATE %s SET "
        + "commit_metadata_payload = :new_commit_metadata_payload, "
        + "commit_metadata_sha1 = :new_commit_metadata_sha1 "
        + "WHERE dataSource = :dataSource AND commit_metadata_sha1 = :old_commit_metadata_sha1",
        dataSourceTable
    );
    this.deleteMetadata = String.format("DELETE from %s WHERE dataSource = :dataSource", dataSourceTable);
    this.resetMetadata = String.format(
        "UPDATE %s SET "
        + "commit_metadata_payload = :new_commit_metadata_payload, "
        + "commit_metadata_sha1 = :new_commit_metadata_sha1 "
        + "WHERE dataSource = :dataSource",
        dataSourceTable
    );
  }

  public IndexerSQLMetadataStorageCoordinator(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector
  )
  {
    this(jsonMapper, dbTables, connector, new NoopEmitter());
  }

  @LifecycleStart
  public void start()
  {
    connector.createDataSourceTable();
    connector.createPendingSegmentsTable();
    connector.createSegmentTable();
  }

  @Override
  public List<DataSegment> getUsedSegmentsForInterval(String dataSource, Interval interval) throws IOException
  {
    return getUsedSegmentsForIntervals(dataSource, ImmutableList.of(interval));
  }

  @Override
  public List<DataSegment> getUsedSegmentsForIntervals(final String dataSource, final List<Interval> intervals)
      throws IOException
  {
    final VersionedIntervalTimeline<String, DataSegment> timeline = connector.retryWithHandle(
        handle -> getTimelineForIntervalsWithHandle(handle, dataSource, intervals)
    );
    final Set<DataSegment> segments = Sets.newHashSet(
        GuavaUtils.explode(GuavaUtils.explode(intervals, i -> timeline.lookup(i)), h -> h.getObject().payloads())
    );
    return new ArrayList<>(segments);
  }

  private List<SegmentIdentifier> getPendingSegmentsForIntervalWithHandle(
      final Handle handle,
      final String dataSource,
      final Interval interval
  ) throws IOException
  {
    final Query<byte[]> query = handle.createQuery(selectPendingsOnInterval)
                                      .bind("dataSource", dataSource)
                                      .bind("start", interval.getStart().toString())
                                      .bind("end", interval.getEnd().toString())
                                      .map(ByteArrayMapper.FIRST);

    final List<SegmentIdentifier> identifiers = Lists.newArrayList();
    try (ResultIterator<byte[]> dbSegments = query.iterator()) {
      while (dbSegments.hasNext()) {
        final byte[] payload = dbSegments.next();
        final SegmentIdentifier identifier = jsonMapper.readValue(payload, SegmentIdentifier.class);

        if (interval.overlaps(identifier.getInterval())) {
          identifiers.add(identifier);
        }
      }
    }
    return identifiers;
  }

  private VersionedIntervalTimeline<String, DataSegment> getTimelineForIntervalsWithHandle(
      final Handle handle,
      final String dataSource,
      final List<Interval> intervals
  ) throws IOException
  {
    return getPayloadsForIntervals(handle, dataSource, intervals, payloads -> {
      final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>();

      while (payloads.hasNext()) {
        final byte[] payload = payloads.next();

        DataSegment segment;
        try {
          segment = jsonMapper.readValue(payload, DataSegment.class);
        }
        catch (Exception e) {
          // should not be happened
          log.warn("Invalid payload in segments table..");
          continue;
        }
        timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpecWithDefault().createChunk(segment));
      }
      return timeline;
    });
  }

  private <T> T getPayloadsForIntervals(
      final Handle handle,
      final String dataSource,
      final List<Interval> intervals,
      final Function<Iterator<byte[]>, T> function
  ) throws IOException
  {
    if (intervals == null || intervals.isEmpty()) {
      throw new IAE("null/empty intervals");
    }

    final StringBuilder sb = new StringBuilder();
    sb.append("SELECT payload FROM ")
      .append(dbTables.getSegmentsTable())
      .append(" WHERE used = true AND dataSource = ? AND (");
    for (int i = 0; i < intervals.size(); i++) {
      sb.append("(start <= ? AND \"end\" >= ?)");
      if (i == intervals.size() - 1) {
        sb.append(")");
      } else {
        sb.append(" OR ");
      }
    }

    Query<Map<String, Object>> query = handle.createQuery(sb.toString())
                                             .bind(0, dataSource);

    for (int i = 0; i < intervals.size(); i++) {
      Interval interval = intervals.get(i);
      query = query.bind(2 * i + 1, interval.getEnd().toString())
                   .bind(2 * i + 2, interval.getStart().toString());
    }

    try (ResultIterator<byte[]> payloads = query.map(ByteArrayMapper.FIRST).iterator()) {
      return function.apply(payloads);
    }
  }

  /**
   * Attempts to insert a set of segments to the database. Returns the set of segments actually added (segments
   * with identifiers already in the database will not be added).
   *
   * @param segments set of segments to add
   *
   * @return set of segments actually added
   */
  public Set<DataSegment> announceHistoricalSegments(final Iterable<DataSegment> segments) throws IOException
  {
    final SegmentPublishResult result = announceHistoricalSegments(segments, null, null);

    // Metadata transaction cannot fail because we are not trying to do one.
    if (!result.isSuccess()) {
      throw new ISE("announceHistoricalSegments failed with null metadata, should not happen.");
    }

    return result.getSegments();
  }

  @Override
  public void announceHistoricalSegmentsWithCheck(final Iterable<DataSegment> segments)
  {
    connector.retryTransaction((handle, status) -> {
      List<DataSegment> list = ImmutableList.copyOf(segments);
      if (segmentExists(handle, list)) {
        throw new ISE("announceHistoricalSegments failed with conflicting segment.");
      }
      // todo : use batch
      for (DataSegment segment : list) {
        insertSegmentWithHandle(handle, segment);
      }
      return true;
    });
  }

  private boolean segmentExists(final Handle handle, final List<DataSegment> segments)
  {
    if (segments.isEmpty()) {
      return false;
    }
    if (segments.size() == 1) {
      return segmentExists(handle, Iterables.getFirst(segments, null));
    }
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT id FROM ").append(dbTables.getSegmentsTable()).append(" WHERE id IN (");
    for (int i = 0; i < segments.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append('?');
    }
    sb.append(')');

    Query<String> query = handle.createQuery(sb.toString())
                                .map(StringMapper.FIRST);
    for (int i = 0; i < segments.size(); i++) {
      query.bind(i, segments.get(i).getIdentifier());
    }
    try (ResultIterator<String> iterator = query.iterator()) {
      return iterator.hasNext();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SegmentPublishResult announceHistoricalSegments(
      final Iterable<DataSegment> segments,
      final DataSourceMetadata startMetadata,
      final DataSourceMetadata endMetadata
  ) throws IOException
  {
    final Iterator<DataSegment> iterator = segments.iterator();
    if (!iterator.hasNext()) {
      throw new IllegalArgumentException("segment set must not be empty");
    }

    final String dataSource = iterator.next().getDataSource();
    if (startMetadata != null) {
      for (DataSegment segment : segments) {
        if (!dataSource.equals(segment.getDataSource())) {
          throw new IllegalArgumentException("segments must all be from the same dataSource when using metadata");
        }
      }
    }

    if ((startMetadata == null && endMetadata != null) || (startMetadata != null && endMetadata == null)) {
      throw new IllegalArgumentException("start/end metadata pair must be either null or non-null");
    }

    final AtomicBoolean txnFailure = new AtomicBoolean(false);

    try {
      return connector.retryTransaction((handle, status) -> {
        if (startMetadata != null) {
          final boolean success = updateDataSourceMetadataWithHandle(handle, dataSource, startMetadata, endMetadata);
          if (!success) {
            status.setRollbackOnly();
            txnFailure.set(true);
            throw new RuntimeException("Aborting transaction!");
          }
        }

        final Set<DataSegment> inserted = Sets.newHashSet();
        for (final DataSegment segment : segments) {
          if (announceHistoricalSegment(handle, segment)) {
            inserted.add(segment);
          }
        }
        return new SegmentPublishResult(ImmutableSet.copyOf(inserted), true);
      });
    }
    catch (CallbackFailedException e) {
      if (txnFailure.get()) {
        return new SegmentPublishResult(ImmutableSet.<DataSegment>of(), false);
      } else {
        throw e;
      }
    }
  }

  @Override
  public SegmentIdentifier allocatePendingSegment(
      final String dataSource,
      final String sequenceName,
      @Nullable final String previousSegmentId,
      final Interval interval,
      final String maxVersion,
      final boolean skipSegmentLineageCheck
  ) throws IOException
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(sequenceName, "sequenceName");
    Preconditions.checkNotNull(interval, "interval");
    Preconditions.checkNotNull(maxVersion, "maxVersion");

    return connector.retryTransaction(
        (handle, status) -> skipSegmentLineageCheck ?
                            allocatePendingSegment(handle, dataSource, sequenceName, interval, maxVersion) :
                            allocatePendingSegmentWithSegmentLineageCheck(
                                handle,
                                dataSource,
                                sequenceName,
                                previousSegmentId,
                                interval,
                                maxVersion
                            )
    );
  }

  @Nullable
  private SegmentIdentifier allocatePendingSegmentWithSegmentLineageCheck(
          final Handle handle,
          final String dataSource,
          final String sequenceName,
          @Nullable final String previousSegmentId,
          final Interval interval,
          final String maxVersion
  ) throws IOException
  {
    final String previousSegmentIdNotNull = previousSegmentId == null ? "" : previousSegmentId;
    final CheckExistingSegmentIdResult result = checkAndGetExistingSegmentId(
            handle.createQuery(selectPending),
            interval,
            sequenceName,
            previousSegmentIdNotNull,
            Pair.of("dataSource", dataSource),
            Pair.of("sequence_name", sequenceName),
            Pair.of("sequence_prev_id", previousSegmentIdNotNull)
    );

    if (result.found) {
      // The found existing segment identifier can be null if its interval doesn't match with the given interval
      return result.segmentIdentifier;
    }

    final SegmentIdentifier newIdentifier = createNewSegment(handle, dataSource, interval, maxVersion);
    if (newIdentifier == null) {
      return null;
    }

    // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
    // Avoiding ON DUPLICATE KEY since it's not portable.
    // Avoiding try/catch since it may cause inadvertent transaction-splitting.

    // UNIQUE key for the row, ensuring sequences do not fork in two directions.
    // Using a single column instead of (sequence_name, sequence_prev_id) as some MySQL storage engines
    // have difficulty with large unique keys (see https://github.com/druid-io/druid/issues/2319)
    final String sequenceNamePrevIdSha1 = BaseEncoding.base16().encode(
            Hashing.sha1()
                    .newHasher()
                    .putBytes(StringUtils.toUtf8(sequenceName))
                    .putByte((byte) 0xff)
                    .putBytes(StringUtils.toUtf8(previousSegmentIdNotNull))
                    .hash()
                    .asBytes()
    );

    insertToPending(
            handle,
            newIdentifier,
            dataSource,
            interval,
            previousSegmentIdNotNull,
            sequenceName,
            sequenceNamePrevIdSha1
    );
    return newIdentifier;
  }

  @Nullable
  private SegmentIdentifier allocatePendingSegment(
          final Handle handle,
          final String dataSource,
          final String sequenceName,
          final Interval interval,
          final String maxVersion
  ) throws IOException
  {
    final CheckExistingSegmentIdResult result = checkAndGetExistingSegmentId(
            handle.createQuery(selectPendingOnInterval),
            interval,
            sequenceName,
            null,
            Pair.of("dataSource", dataSource),
            Pair.of("sequence_name", sequenceName),
            Pair.of("start", interval.getStart().toString()),
            Pair.of("end", interval.getEnd().toString())
    );

    if (result.found) {
      // The found existing segment identifier can be null if its interval doesn't match with the given interval
      return result.segmentIdentifier;
    }

    final SegmentIdentifier newIdentifier = createNewSegment(handle, dataSource, interval, maxVersion);
    if (newIdentifier == null) {
      return null;
    }

    // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
    // Avoiding ON DUPLICATE KEY since it's not portable.
    // Avoiding try/catch since it may cause inadvertent transaction-splitting.

    // UNIQUE key for the row, ensuring we don't have more than one segment per sequence per interval.
    // Using a single column instead of (sequence_name, sequence_prev_id) as some MySQL storage engines
    // have difficulty with large unique keys (see https://github.com/druid-io/druid/issues/2319)
    final String sequenceNamePrevIdSha1 = BaseEncoding.base16().encode(
            Hashing.sha1()
                    .newHasher()
                    .putBytes(StringUtils.toUtf8(sequenceName))
                    .putByte((byte) 0xff)
                    .putLong(interval.getStartMillis())
                    .putLong(interval.getEndMillis())
                    .hash()
                    .asBytes()
    );

    // always insert empty previous sequence id
    insertToPending(handle, newIdentifier, dataSource, interval, "", sequenceName, sequenceNamePrevIdSha1);

    log.info(
            "Allocated pending segment [%s] for sequence[%s] in DB",
            newIdentifier.getIdentifierAsString(),
            sequenceName
    );

    return newIdentifier;
  }

  @SafeVarargs
  private final CheckExistingSegmentIdResult checkAndGetExistingSegmentId(
      final Query<Map<String, Object>> query,
      final Interval interval,
      final String sequenceName,
      final @Nullable String previousSegmentId,
      final Pair<String, String>... queryVars
  ) throws IOException
  {
    Query<Map<String, Object>> boundQuery = query;
    for (Pair<String, String> var : queryVars) {
      boundQuery = boundQuery.bind(var.lhs, var.rhs);
    }
    final List<byte[]> existingBytes = boundQuery.map(ByteArrayMapper.FIRST).list();

    if (!existingBytes.isEmpty()) {
      final SegmentIdentifier existingIdentifier = jsonMapper.readValue(
              Iterables.getOnlyElement(existingBytes),
              SegmentIdentifier.class
      );

      if (existingIdentifier.getInterval().getStartMillis() == interval.getStartMillis()
              && existingIdentifier.getInterval().getEndMillis() == interval.getEndMillis()) {
        if (previousSegmentId == null) {
          log.info(
                  "Found existing pending segment [%s] for sequence[%s] in DB",
                  existingIdentifier.getIdentifierAsString(),
                  sequenceName
          );
        } else {
          log.info(
                  "Found existing pending segment [%s] for sequence[%s] (previous = [%s]) in DB",
                  existingIdentifier.getIdentifierAsString(),
                  sequenceName,
                  previousSegmentId
          );
        }

        return new CheckExistingSegmentIdResult(true, existingIdentifier);
      } else {
        if (previousSegmentId == null) {
          log.warn(
                  "Cannot use existing pending segment [%s] for sequence[%s] in DB, "
                          + "does not match requested interval[%s]",
                  existingIdentifier.getIdentifierAsString(),
                  sequenceName,
                  interval
          );
        } else {
          log.warn(
                  "Cannot use existing pending segment [%s] for sequence[%s] (previous = [%s]) in DB, "
                          + "does not match requested interval[%s]",
                  existingIdentifier.getIdentifierAsString(),
                  sequenceName,
                  previousSegmentId,
                  interval
          );
        }

        return new CheckExistingSegmentIdResult(true, null);
      }
    }
    return new CheckExistingSegmentIdResult(false, null);
  }

  private static class CheckExistingSegmentIdResult
  {
    private final boolean found;
    @Nullable
    private final SegmentIdentifier segmentIdentifier;

    CheckExistingSegmentIdResult(boolean found, @Nullable SegmentIdentifier segmentIdentifier)
    {
      this.found = found;
      this.segmentIdentifier = segmentIdentifier;
    }
  }

  private void insertToPending(
          Handle handle,
          SegmentIdentifier newIdentifier,
          String dataSource,
          Interval interval,
          String previousSegmentId,
          String sequenceName,
          String sequenceNamePrevIdSha1
  ) throws JsonProcessingException
  {
    handle.createStatement(insertPending)
          .bind("id", newIdentifier.getIdentifierAsString())
          .bind("dataSource", dataSource)
          .bind("created_date", DateTimes.nowUtc().toString())
          .bind("start", interval.getStart().toString())
          .bind("end", interval.getEnd().toString())
          .bind("sequence_name", sequenceName)
          .bind("sequence_prev_id", previousSegmentId)
          .bind("sequence_name_prev_id_sha1", sequenceNamePrevIdSha1)
          .bind("payload", jsonMapper.writeValueAsBytes(newIdentifier))
          .execute();
  }

  @Nullable
  private SegmentIdentifier createNewSegment(
          final Handle handle,
          final String dataSource,
          final Interval interval,
          final String maxVersion
  ) throws IOException
  {
    // Make up a pending segment based on existing segments and pending segments in the DB. This works
    // assuming that all tasks inserting segments at a particular point in time are going through the
    // allocatePendingSegment flow. This should be assured through some other mechanism (like task locks).

    final List<TimelineObjectHolder<String, DataSegment>> existingChunks = getTimelineForIntervalsWithHandle(
            handle,
            dataSource,
            ImmutableList.of(interval)
    ).lookup(interval);

    if (existingChunks.size() > 1) {
      // Not possible to expand more than one chunk with a single segment.
      log.warn(
              "Cannot allocate new segment for dataSource[%s], interval[%s], maxVersion[%s]: already have [%,d] chunks.",
              dataSource,
              interval,
              maxVersion,
              existingChunks.size()
      );
      return null;
    } else {
      SegmentIdentifier max = null;

      if (!existingChunks.isEmpty()) {
        TimelineObjectHolder<String, DataSegment> existingHolder = Iterables.getOnlyElement(existingChunks);
        for (PartitionChunk<DataSegment> existing : existingHolder.getObject()) {
          if (max == null || max.getShardSpec().getPartitionNum() < existing.getObject()
                  .getShardSpec()
                  .getPartitionNum()) {
            max = SegmentIdentifier.fromDataSegment(existing.getObject());
          }
        }
      }

      final List<SegmentIdentifier> pendings = getPendingSegmentsForIntervalWithHandle(
              handle,
              dataSource,
              interval
      );

      for (SegmentIdentifier pending : pendings) {
        if (max == null ||
                pending.getVersion().compareTo(max.getVersion()) > 0 ||
                (pending.getVersion().equals(max.getVersion())
                 && pending.getShardSpec().getPartitionNum() > max.getShardSpec().getPartitionNum())) {
          max = pending;
        }
      }

      if (max == null) {
        return new SegmentIdentifier(
                dataSource,
                interval,
                maxVersion,
                new NumberedShardSpec(0, 0)
        );
      } else if (!max.getInterval().equals(interval) || max.getVersion().compareTo(maxVersion) > 0) {
        log.warn(
                "Cannot allocate new segment for dataSource[%s], interval[%s], maxVersion[%s]: conflicting segment[%s].",
                dataSource,
                interval,
                maxVersion,
                max.getIdentifierAsString()
        );
        return null;
      } else if (max.getShardSpec() instanceof LinearShardSpec) {
        return new SegmentIdentifier(
                dataSource,
                max.getInterval(),
                max.getVersion(),
                new LinearShardSpec(max.getShardSpec().getPartitionNum() + 1)
        );
      } else if (max.getShardSpec() instanceof NumberedShardSpec) {
        return new SegmentIdentifier(
                dataSource,
                max.getInterval(),
                max.getVersion(),
                new NumberedShardSpec(
                        max.getShardSpec().getPartitionNum() + 1,
                        ((NumberedShardSpec) max.getShardSpec()).getPartitions()
                )
        );
      } else {
        log.warn(
                "Cannot allocate new segment for dataSource[%s], interval[%s], maxVersion[%s]: ShardSpec class[%s] used by [%s].",
                dataSource,
                interval,
                maxVersion,
                max.getShardSpec().getClass(),
                max.getIdentifierAsString()
        );
        return null;
      }
    }
  }

  @Override
  public boolean updateDataSourceMetadata(final String dataSource, final DataSourceMetadata metaData)
  {
    return connector.retryTransaction(
        (handle, status) -> updateDataSourceMetadataWithHandle(handle, dataSource, metaData, metaData)
    );
  }

  @Override
  public int deletePendingSegments(String dataSource, Interval deleteInterval)
  {
    return connector.getDBI().inTransaction(
        (handle, status) -> handle.createStatement(deletePendings)
                                  .bind("dataSource", dataSource)
                                  .bind("start", deleteInterval.getStart().toString())
                                  .bind("end", deleteInterval.getEnd().toString())
                                  .execute()
    );
  }

  /**
   * Attempts to insert a single segment to the database. If the segment already exists, will do nothing; although,
   * this checking is imperfect and callers must be prepared to retry their entire transaction on exceptions.
   *
   * @return true if the segment was added, false if it already existed
   */
  private boolean announceHistoricalSegment(final Handle handle, final DataSegment segment) throws IOException
  {
    try {
      if (segmentExists(handle, segment)) {
        log.info("Found [%s] in DB, not updating DB", segment.getIdentifier());
        return false;
      }

      // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
      // Avoiding ON DUPLICATE KEY since it's not portable.
      // Avoiding try/catch since it may cause inadvertent transaction-splitting.
      insertSegmentWithHandle(handle, segment);
    }
    catch (Exception e) {
      log.error(e, "Exception inserting segment [%s] into DB", segment.getIdentifier());
      throw e;
    }

    emitter.emit(
        new Events.SimpleEvent(
            ImmutableMap.<String, Object>of(
                "feed", "IndexerSQLMetadataStorageCoordinator",
                "type", "segmentAnnounced",
                "createdDate", System.currentTimeMillis(),
                "payload", segment
            )
        )
    );

    return true;
  }

  private void insertSegmentWithHandle(Handle handle, DataSegment segment) throws JsonProcessingException
  {
    handle.createStatement(insertSegment)
          .bind("id", segment.getIdentifier())
          .bind("dataSource", segment.getDataSource())
          .bind("created_date", new DateTime().toString())
          .bind("start", segment.getInterval().getStart().toString())
          .bind("end", segment.getInterval().getEnd().toString())
          .bind("partitioned", !(segment.getShardSpecWithDefault() instanceof NoneShardSpec))
          .bind("version", segment.getVersion())
          .bind("used", true)
          .bind("payload", jsonMapper.writeValueAsBytes(segment))
          .execute();

    log.info("Published segment [%s] to DB", segment.getIdentifier());
  }

  private boolean segmentExists(final Handle handle, final DataSegment segment)
  {
    Query<String> query = handle.createQuery(selectId)
                                .bind("identifier", segment.getIdentifier())
                                .map(StringMapper.FIRST);
    try (ResultIterator<String> iterator = query.iterator()) {
      return iterator.hasNext();
    }
  }

  /**
   * Read dataSource metadata. Returns null if there is no metadata.
   */
  @Override
  public DataSourceMetadata getDataSourceMetadata(final String dataSource)
  {
    final byte[] bytes = connector.lookup(
        dbTables.getDataSourceTable(),
        "dataSource",
        "commit_metadata_payload",
        dataSource
    );

    if (bytes == null) {
      return null;
    }

    try {
      return jsonMapper.readValue(bytes, DataSourceMetadata.class);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Read dataSource metadata as bytes, from a specific handle. Returns null if there is no metadata.
   */
  private byte[] getDataSourceMetadataWithHandleAsBytes(
      final Handle handle,
      final String dataSource
  )
  {
    return connector.lookupWithHandle(
        handle,
        dbTables.getDataSourceTable(),
        "dataSource",
        "commit_metadata_payload",
        dataSource
    );
  }

  /**
   * Compare-and-swap dataSource metadata in a transaction. This will only modify dataSource metadata if it equals
   * oldCommitMetadata when this function is called (based on T.equals). This method is idempotent in that if
   * the metadata already equals newCommitMetadata, it will return true.
   *
   * @param handle        database handle
   * @param dataSource    druid dataSource
   * @param startMetadata dataSource metadata pre-insert must match this startMetadata according to
   *                      {@link DataSourceMetadata#matches(DataSourceMetadata)}
   * @param endMetadata   dataSource metadata post-insert will have this endMetadata merged in with
   *                      {@link DataSourceMetadata#plus(DataSourceMetadata)}
   *
   * @return true if dataSource metadata was updated from matching startMetadata to matching endMetadata
   */
  private boolean updateDataSourceMetadataWithHandle(
      final Handle handle,
      final String dataSource,
      final DataSourceMetadata startMetadata,
      final DataSourceMetadata endMetadata
  ) throws IOException
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(startMetadata, "startMetadata");
    Preconditions.checkNotNull(endMetadata, "endMetadata");

    final byte[] oldCommitMetadataBytesFromDb = getDataSourceMetadataWithHandleAsBytes(handle, dataSource);
    final String oldCommitMetadataSha1FromDb;
    final DataSourceMetadata oldCommitMetadataFromDb;

    if (oldCommitMetadataBytesFromDb == null) {
      oldCommitMetadataSha1FromDb = null;
      oldCommitMetadataFromDb = null;
    } else {
      oldCommitMetadataSha1FromDb = BaseEncoding.base16().encode(
          Hashing.sha1().hashBytes(oldCommitMetadataBytesFromDb).asBytes()
      );
      oldCommitMetadataFromDb = jsonMapper.readValue(oldCommitMetadataBytesFromDb, DataSourceMetadata.class);
    }

    final boolean startMetadataMatchesExisting = oldCommitMetadataFromDb == null
                                                 ? startMetadata.isValidStart()
                                                 : startMetadata.matches(oldCommitMetadataFromDb);

    if (!startMetadataMatchesExisting) {
      // Not in the desired start state.
      log.info("Not updating metadata, existing state is not the expected start state.");
      log.debug("Existing database state [%s], request's start metadata [%s]", oldCommitMetadataFromDb, startMetadata);
      return false;
    }

    final DataSourceMetadata newCommitMetadata = oldCommitMetadataFromDb == null
                                                 ? endMetadata
                                                 : oldCommitMetadataFromDb.plus(endMetadata);
    final byte[] newCommitMetadataBytes = jsonMapper.writeValueAsBytes(newCommitMetadata);
    final String newCommitMetadataSha1 = BaseEncoding.base16().encode(
        Hashing.sha1().hashBytes(newCommitMetadataBytes).asBytes()
    );

    final int numRows;
    if (oldCommitMetadataBytesFromDb == null) {
      // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
      numRows = handle.createStatement(insertMetadata)
                      .bind("dataSource", dataSource)
                      .bind("created_date", new DateTime().toString())
                      .bind("commit_metadata_payload", newCommitMetadataBytes)
                      .bind("commit_metadata_sha1", newCommitMetadataSha1)
                      .execute();

    } else {
      // Expecting a particular old metadata; use the SHA1 in a compare-and-swap UPDATE
      numRows = handle.createStatement(updateMetadata)
                      .bind("dataSource", dataSource)
                      .bind("old_commit_metadata_sha1", oldCommitMetadataSha1FromDb)
                      .bind("new_commit_metadata_payload", newCommitMetadataBytes)
                      .bind("new_commit_metadata_sha1", newCommitMetadataSha1)
                      .execute();
    }
    final boolean retVal = numRows > 0;

    if (retVal) {
      log.info("Updated metadata from[%s] to[%s].", oldCommitMetadataFromDb, newCommitMetadata);
    } else {
      log.info("Not updating metadata, compare-and-swap failure.");
    }

    return retVal;
  }

  @Override
  public void updateSegments(final Iterable<DataSegment> segments) throws IOException
  {
    connector.getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            for (final DataSegment segment : segments) {
              updatePayload(handle, segment);
            }

            return null;
          }
        }
    );
  }

  @Override
  public void deleteSegments(final Iterable<String> ids) throws IOException
  {
    connector.getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws IOException
          {
            for (String id : ids) {
              deleteSegment(handle, id);
            }
            return null;
          }
        }
    );
  }

  private void deleteSegment(final Handle handle, final String id)
  {
    handle.createStatement(deleteSegment)
          .bind("id", id)
          .execute();
  }

  private void updatePayload(final Handle handle, final DataSegment segment) throws IOException
  {
    try {
      handle.createStatement(updatePayload)
            .bind("id", segment.getIdentifier())
            .bind("payload", jsonMapper.writeValueAsBytes(segment))
            .execute();
    }
    catch (IOException e) {
      log.error(e, "Exception inserting into DB");
      throw e;
    }
  }

  @Override
  public List<DataSegment> getUnusedSegmentsForInterval(final String dataSource, final Interval interval)
  {
    List<DataSegment> matchingSegments = connector.getDBI().withHandle(
        new HandleCallback<List<DataSegment>>()
        {
          @Override
          public List<DataSegment> withHandle(Handle handle) throws IOException, SQLException
          {
            return handle
                .createQuery(selectPayload)
                .bind("dataSource", dataSource)
                .bind("start", interval.getStart().toString())
                .bind("end", interval.getEnd().toString())
                .map(ByteArrayMapper.FIRST)
                .fold(
                    Lists.<DataSegment>newArrayList(),
                    new Folder3<List<DataSegment>, byte[]>()
                    {
                      @Override
                      public List<DataSegment> fold(
                          List<DataSegment> accumulator,
                          byte[] payload,
                          FoldController foldController,
                          StatementContext statementContext
                      ) throws SQLException
                      {
                        try {
                          accumulator.add(jsonMapper.readValue(payload, DataSegment.class));
                          return accumulator;
                        }
                        catch (Exception e) {
                          throw Throwables.propagate(e);
                        }
                      }
                    }
                );
          }
        }
    );

    log.info("Found %,d segments for %s for interval %s.", matchingSegments.size(), dataSource, interval);
    return matchingSegments;
  }

  @Override
  public boolean deleteDataSourceMetadata(final String dataSource)
  {
    return connector.retryWithHandle(
        handle -> handle.createStatement(deleteMetadata)
                        .bind("dataSource", dataSource)
                        .execute() > 0
    );
  }

  @Override
  public boolean resetDataSourceMetadata(final String dataSource, DataSourceMetadata dataSourceMetadata)
      throws IOException
  {
    final byte[] newCommitMetadataBytes = jsonMapper.writeValueAsBytes(dataSourceMetadata);
    final String newCommitMetadataSha1 = BaseEncoding.base16().encode(
        Hashing.sha1().hashBytes(newCommitMetadataBytes).asBytes()
    );
    return connector.retryWithHandle(
        handle -> handle.createStatement(resetMetadata)
                        .bind("dataSource", dataSource)
                        .bind("new_commit_metadata_payload", newCommitMetadataBytes)
                        .bind("new_commit_metadata_sha1", newCommitMetadataSha1)
                        .execute() == 1
    );
  }
}
