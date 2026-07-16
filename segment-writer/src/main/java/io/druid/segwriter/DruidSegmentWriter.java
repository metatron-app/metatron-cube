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

package io.druid.segwriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.SegmentUtils;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Builds a single Druid segment from a set of rows entirely in-process — no ingestion task,
 * no Hadoop, no MapReduce — and pushes it to deep storage via a {@link DataSegmentPusher}.
 *
 * Intended to run inside an external engine (e.g. a Spark executor): read your source
 * (parquet, etc.) natively, hand the rows here per partition, and collect the returned
 * {@link DataSegment}s to publish (overlord API) from the driver.
 *
 * Construction uses no Guice; the segment format matches this build's historicals because it
 * uses this build's {@link IndexMergerV9}/{@link IndexIO}.
 */
public final class DruidSegmentWriter
{
  private DruidSegmentWriter() {}

  /**
   * @param spec      schema (datasource, dimensions, metrics, granularity, rollup, ts column)
   * @param interval  the segment's time interval
   * @param version   segment version string (e.g. an ISO timestamp); also the dedup key
   * @param shardSpec partition shard spec (e.g. NoneShardSpec or a hash/linear shard)
   * @param rows      input rows; each must carry the timestamp column (epoch millis as a Number)
   * @param pusher    deep-storage pusher (see {@link DataSegmentPushers})
   * @param tmpDir    scratch dir for the persisted segment
   * @return the published {@link DataSegment} with loadSpec/size/binaryVersion filled in
   */
  public static DataSegment write(
      SegmentSpec spec,
      Interval interval,
      String version,
      ShardSpec shardSpec,
      Iterable<Map<String, Object>> rows,
      DataSegmentPusher pusher,
      File tmpDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    final Persisted p = persist(spec, interval, version, shardSpec, rows, tmpDir, indexSpec);
    final long tPush = System.nanoTime();
    final DataSegment result = pusher.push(p.dir, p.template);   // S3 upload of the segment files
    Prof.pushNanos.addAndGet(System.nanoTime() - tPush);
    return result;
  }

  /** A persisted (but not yet pushed) segment: the on-disk v9 dir + a template DataSegment (loadSpec/size unfilled). */
  public static final class Persisted
  {
    public final File dir;
    public final DataSegment template;

    public Persisted(File dir, DataSegment template)
    {
      this.dir = dir;
      this.template = template;
    }
  }

  /**
   * Accumulate + persist only (no push), so callers can push asynchronously (overlap S3 upload with the next
   * chunk's fetch/persist). Same body as {@link #write} minus the terminal {@code pusher.push}.
   *
   * <p>Convenience for the one-thread case: {@link #newIndex} + {@link #addRow} per row + {@link #persistIndex}.
   * A caller that must keep reading its source while a filled index is written should use those directly.
   */
  public static Persisted persist(
      SegmentSpec spec,
      Interval interval,
      String version,
      ShardSpec shardSpec,
      Iterable<Map<String, Object>> rows,
      File tmpDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    final long tStart = System.nanoTime();
    final IncrementalIndex index = newIndex(spec, interval);
    boolean accumulated = false;
    try {
      for (Map<String, Object> row : rows) {
        addRow(index, spec, row);
      }
      // accumulate = pulling rows from the source iterator (e.g. a Trino fetch) + index.add (the sorted TreeMap insert)
      Prof.accumulateNanos.addAndGet(System.nanoTime() - tStart);
      accumulated = true;
    }
    finally {
      if (!accumulated) {
        closeQuietly(index);   // on success persistIndex closes it
      }
    }
    return persistIndex(spec, index, interval, version, shardSpec, tmpDir, indexSpec);
  }

  /**
   * Open an empty index for one segment (the accumulate half of {@link #persist}).
   *
   * <p>Split out so a caller can keep draining its source on one thread while a previously filled index is written
   * on another. Fused, the two serialize: the source sits unread for the whole (columns + lucene) write, which for
   * a network source (Trino) leaves the server-side query stalled with a full output buffer.
   */
  public static IncrementalIndex newIndex(SegmentSpec spec, Interval interval)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(interval.getStartMillis())
        .withQueryGranularity(spec.getQueryGranularity())
        .withDimensionsSpec(new DimensionsSpec(DimensionsSpec.getDefaultSchemas(spec.getDimensions()), null, null))
        .withMetrics(spec.getMetrics())
        .withRollup(spec.isRollup())
        .build();
    return new OnheapIncrementalIndex(schema, true, Integer.MAX_VALUE);
  }

  /** Add one source row to an index from {@link #newIndex}. */
  public static void addRow(IncrementalIndex index, SegmentSpec spec, Map<String, Object> row)
  {
    final Object ts = row.get(spec.getTimestampColumn());
    if (!(ts instanceof Number)) {
      throw new IllegalArgumentException(
          "row timestamp column [" + spec.getTimestampColumn() + "] must be epoch-millis Number, got: " + ts
      );
    }
    index.add(new MapBasedInputRow(((Number) ts).longValue(), spec.getDimensions(), row));
  }

  /**
   * Persist a filled index and close it (the persist half of {@link #persist}): the v9 columnar write, incl. the
   * lucene index for secondary-indexed columns. The index is closed even if the write fails.
   */
  public static Persisted persistIndex(
      SegmentSpec spec,
      IncrementalIndex index,
      Interval interval,
      String version,
      ShardSpec shardSpec,
      File tmpDir,
      IndexSpec indexSpec
  ) throws IOException
  {
    // indexMapper knows the lucene column part serde subtypes, so secondary-indexed columns
    // round-trip through IndexMergerV9/IndexIO (write + read-back).
    final ObjectMapper mapper = Json.indexMapper();
    final IndexIO indexIO = new IndexIO(mapper);
    final IndexMergerV9 merger = new IndexMergerV9(mapper, indexIO);

    final File persisted;
    final int numRows;
    final long tStart = System.nanoTime();
    try (IncrementalIndex toClose = index) {
      numRows = index.size();   // post-rollup row count for the segment metadata
      persisted = merger.persist(
          index,
          interval,
          new File(tmpDir, "seg-" + UUID.randomUUID()),
          indexSpec
      );
      Prof.persistNanos.addAndGet(System.nanoTime() - tStart);
    }

    final List<String> metricNames = new ArrayList<>();
    for (AggregatorFactory f : spec.getMetrics()) {
      metricNames.add(f.getName());
    }

    final DataSegment template = new DataSegment(
        spec.getDataSource(),
        interval,
        version,
        ImmutableMap.<String, Object>of(),     // loadSpec filled by the pusher
        spec.getDimensions(),
        metricNames,
        shardSpec,
        SegmentUtils.getVersionFromDir(persisted),
        0L,                                     // size filled by the pusher
        numRows
    );
    return new Persisted(persisted, template);
  }

  private static void closeQuietly(IncrementalIndex index)
  {
    try {
      index.close();
    }
    catch (Exception ignored) {
      // best-effort: we are already unwinding a failure
    }
  }

  /** Cumulative phase timings across all segment builds in this JVM (nanos). Read/reset by a driver for profiling. */
  public static final class Prof
  {
    public static final java.util.concurrent.atomic.AtomicLong accumulateNanos = new java.util.concurrent.atomic.AtomicLong();
    public static final java.util.concurrent.atomic.AtomicLong persistNanos = new java.util.concurrent.atomic.AtomicLong();
    public static final java.util.concurrent.atomic.AtomicLong pushNanos = new java.util.concurrent.atomic.AtomicLong();

    private Prof() {}
  }
}
