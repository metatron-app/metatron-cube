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

package io.druid.segwriter.trino;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.Files;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segwriter.DruidSegmentWriter;
import io.druid.segwriter.Json;
import io.druid.segwriter.SegmentIngestSpec;
import io.druid.segwriter.SegmentIngestor;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-process, Spark-free daily re-index: stream the source from Trino in {@code timestamp_trigger} order and
 * roll one segment per day (splitting a day at {@code maxRowsPerSegment}). The source iceberg table is partitioned
 * by {@code hour(timestamp_trigger)}, so a per-day query with a {@code timestamp_trigger} bound is partition-pruned
 * (no full scan) and needs no shuffle — the rows arrive already time-grouped, so we just feed them to the same
 * {@link SegmentIngestor#buildSegment} the Spark path used and push s3_smoosh segments to deep storage.
 *
 * <p>Config: a {@link SegmentIngestSpec} JSON (dims/metrics/secondaryIndexing/segmentGranularity/maxRowsPerSegment/
 * bucket/baseKey/endpoint/storageType, with {@code timestampColumn=timestamp_trigger}). Trino connection from env:
 * {@code TRINO_HOST} (default trino.op.s2dev.net:443), {@code TRINO_CATALOG} (default iceberg_prod), schema fixed to
 * {@code ato}, {@code TRINO_USER} (label only — no auth), {@code TRINO_TABLE} (default the atom table). Deep-storage
 * push uses the default AWS credential chain (AWS_* env), same as the Spark path.
 *
 * <p>Usage: {@code TrinoIngestion <spec.json> <startDate yyyy-MM-dd> <endDate yyyy-MM-dd exclusive>}
 */
public final class TrinoIngestion
{
  private static final ObjectMapper MAPPER = Json.mapper();
  private static final java.util.concurrent.atomic.AtomicLong FETCH_NANOS = new java.util.concurrent.atomic.AtomicLong();
  private static final java.util.concurrent.atomic.AtomicLong NEXT_NANOS = new java.util.concurrent.atomic.AtomicLong();   // rs.next(): page wait + decode
  private static final java.util.concurrent.atomic.AtomicLong DESER_NANOS = new java.util.concurrent.atomic.AtomicLong();  // getObject(): read values

  private static long ms(long nanos) { return nanos / 1_000_000L; }

  public static void main(String[] args) throws Exception
  {
    // rows arrive ORDER BY timestamp_trigger, so the OnheapIncrementalIndex can append instead of sort-inserting
    // (set before OnheapIncrementalIndex is first loaded). rollup=false gates it further inside the index.
    System.setProperty("druid.incrementalIndex.presortedAppend", "true");
    if (args.length < 3) {
      System.err.println("usage: TrinoIngestion <spec.json> <startDate yyyy-MM-dd> <endDate yyyy-MM-dd (exclusive)>");
      System.exit(2);
    }
    final SegmentIngestSpec spec = MAPPER.readValue(
        java.nio.file.Files.readAllBytes(Paths.get(args[0])), SegmentIngestSpec.class);
    final LocalDate start = LocalDate.parse(args[1]);
    final LocalDate end = LocalDate.parse(args[2]);

    final String host = env("TRINO_HOST", "trino.op.s2dev.net:443");
    final String catalog = env("TRINO_CATALOG", "iceberg_prod");
    final String schema = "ato";
    final String user = env("TRINO_USER", "ulp-parse-sink");
    final String table = env("TRINO_TABLE", "atom_analysis_credential_v2");
    final String tsCol = spec.getTimestampColumn();
    final int maxRows = spec.getMaxRowsPerSegment();
    final int chunkHours = Integer.parseInt(env("CHUNK_HOURS", "4"));
    final int workers = Integer.parseInt(env("WORKERS", "8"));

    // columns to SELECT: timestamp + dimensions + every metric's source column(s). (count has none.)
    final LinkedHashSet<String> colSet = new LinkedHashSet<>();
    colSet.add(tsCol);
    colSet.addAll(spec.getDimensions());
    for (AggregatorFactory m : spec.getMetrics()) {
      final List<String> req = m.requiredFields();
      if (req != null) {
        colSet.addAll(req);
      }
    }
    final List<String> cols = new ArrayList<>(colSet);
    final String colList = String.join(", ", cols);

    final String jdbcUrl = "jdbc:trino://" + host + "/" + catalog + "/" + schema
                           + (host.endsWith(":443") ? "?SSL=true" : "");
    final Properties props = new Properties();
    props.setProperty("user", user);

    final String version = new DateTime().toString();   // one version for the whole run; > the HOUR segments' version
    final DataSegmentPusher pusher = SegmentIngestor.pusher(spec);

    // split [start, end) into fixed time-chunks; each chunk = one parallel task = one segment interval
    final long startMs = start.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    final long endMs = end.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    final long chunkMs = chunkHours * 3_600_000L;
    final List<long[]> chunks = new ArrayList<>();
    for (long c0 = startMs; c0 < endMs; c0 += chunkMs) {
      chunks.add(new long[]{c0, Math.min(c0 + chunkMs, endMs)});
    }
    final int nChunks = chunks.size();

    System.out.println("trino-ingestion: url=" + jdbcUrl + " table=" + table + " tsCol=" + tsCol
                       + " range=[" + start + "," + end + ") chunkHours=" + chunkHours + " chunks=" + nChunks
                       + " workers=" + workers + " maxRows=" + maxRows + " version=" + version);

    final AtomicInteger totalSegs = new AtomicInteger();
    final AtomicLong totalRows = new AtomicLong();
    final AtomicInteger doneChunks = new AtomicInteger();
    final long wallStart = System.nanoTime();

    // across-chunk MT: W workers each run whole chunks (own Trino connection); while one chunk persists
    // (CPU: lucene) another fetches (I/O) -> fetch+build+persist overlap across chunks.
    final ExecutorService pool = Executors.newFixedThreadPool(workers);
    final List<Future<?>> futures = new ArrayList<>();
    for (final long[] c : chunks) {
      futures.add(pool.submit(() -> {
        processChunk(c[0], c[1], spec, jdbcUrl, props, table, colList, cols, tsCol, maxRows, version,
                     pusher, totalSegs, totalRows, doneChunks, nChunks);
        return null;
      }));
    }
    pool.shutdown();
    for (Future<?> f : futures) {
      f.get();   // surface any chunk failure (aborts the run)
    }

    final long wallMs = ms(System.nanoTime() - wallStart);
    final long fetchMs = ms(FETCH_NANOS.get());
    final long addMs = ms(DruidSegmentWriter.Prof.accumulateNanos.get()) - fetchMs;
    final long persistMs = ms(DruidSegmentWriter.Prof.persistNanos.get());
    final long pushMs = ms(DruidSegmentWriter.Prof.pushNanos.get());
    System.out.println("trino-ingestion: DONE. built " + totalSegs.get() + " segment(s), " + totalRows.get()
                       + " rows in " + wallMs + "ms ("
                       + (totalRows.get() * 1000L / Math.max(1, wallMs)) + " rows/s, " + workers + " workers)");
    System.out.println("  AGGREGATE across threads (ms, summed over workers so > wall): fetch=" + fetchMs
                       + " [next=" + ms(NEXT_NANOS.get()) + " deser=" + ms(DESER_NANOS.get()) + "]"
                       + " index-add=" + addMs + " persist(cols+lucene)=" + persistMs + " s3-push=" + pushMs);
  }

  private static final DateTimeFormatter TS_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

  /** Process one time-chunk on its own Trino connection: query -> build shards (interval = the chunk) -> push. */
  private static void processChunk(
      long c0, long c1, SegmentIngestSpec spec, String jdbcUrl, Properties props, String table,
      String colList, List<String> cols, String tsCol, int maxRows, String version,
      DataSegmentPusher pusher, AtomicInteger totalSegs, AtomicLong totalRows, AtomicInteger doneChunks, int nChunks
  ) throws Exception
  {
    final String c0s = TS_FMT.format(Instant.ofEpochMilli(c0));
    final String c1s = TS_FMT.format(Instant.ofEpochMilli(c1));
    final String sql = "SELECT " + colList + " FROM " + table
                       + " WHERE " + tsCol + " >= TIMESTAMP '" + c0s + " UTC'"
                       + " AND " + tsCol + " < TIMESTAMP '" + c1s + " UTC'"
                       + " ORDER BY " + tsCol + " ASC";
    final Interval iv = new Interval(c0, c1);
    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {
      stmt.setFetchSize(10_000);
      try (ResultSet rs = stmt.executeQuery(sql)) {
        final PeekingIterator<Map<String, Object>> rows = Iterators.peekingIterator(rowIterator(rs, cols, tsCol));
        int shard = 0;
        long chunkRows = 0;
        while (rows.hasNext()) {
          final int[] emitted = {0};   // ordered by ts, so each maxRows shard is time-contiguous
          final Iterator<Map<String, Object>> shardIt = new Iterator<Map<String, Object>>()
          {
            @Override public boolean hasNext() { return emitted[0] < maxRows && rows.hasNext(); }
            @Override public Map<String, Object> next() { emitted[0]++; return rows.next(); }
          };
          final File tmp = Files.createTempDir();
          try {
            final DataSegment seg = SegmentIngestor.buildSegment(spec, iv, version, shard, 2, shardIt, tmp, pusher);
            totalSegs.incrementAndGet();
            totalRows.addAndGet(seg.getNumRows());
            chunkRows += seg.getNumRows();
            System.out.println("  built " + seg.getIdentifier() + " rows=" + seg.getNumRows() + " size=" + seg.getSize());
          }
          finally {
            FileUtils.deleteQuietly(tmp);   // free the built segment's scratch (else the pod fills up)
          }
          shard++;
        }
        final int done = doneChunks.incrementAndGet();
        System.out.println("[chunk " + done + "/" + nChunks + " " + c0s + "] " + shard + " shard(s), " + chunkRows
                           + " rows (running: " + totalSegs.get() + " segs, " + totalRows.get() + " rows)");
      }
    }
  }

  /** One-shot iterator over a ResultSet -> row maps (timestamp column normalized to epoch-millis Long). */
  private static Iterator<Map<String, Object>> rowIterator(ResultSet rs, List<String> cols, String tsCol)
  {
    return new Iterator<Map<String, Object>>()
    {
      private Boolean hasNext = null;

      @Override
      public boolean hasNext()
      {
        if (hasNext == null) {
          final long t = System.nanoTime();
          try {
            hasNext = rs.next();
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
          final long d = System.nanoTime() - t;
          NEXT_NANOS.addAndGet(d);
          FETCH_NANOS.addAndGet(d);   // Trino page fetch / row advance (wait + page decode)
        }
        return hasNext;
      }

      @Override
      public Map<String, Object> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        hasNext = null;
        final long t = System.nanoTime();
        final Map<String, Object> row = new java.util.HashMap<>();
        try {
          for (String c : cols) {
            final Object v = rs.getObject(c);
            row.put(c, c.equals(tsCol) ? toMillis(v) : v);
          }
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
        final long d = System.nanoTime() - t;
        DESER_NANOS.addAndGet(d);
        FETCH_NANOS.addAndGet(d);   // column deserialize
        return row;
      }
    };
  }

  private static long toMillis(Object v)
  {
    if (v == null) {
      throw new IllegalArgumentException("null timestamp");
    }
    if (v instanceof Number) {
      return ((Number) v).longValue();
    }
    if (v instanceof java.sql.Timestamp) {
      return ((java.sql.Timestamp) v).getTime();
    }
    if (v instanceof Instant) {
      return ((Instant) v).toEpochMilli();
    }
    if (v instanceof OffsetDateTime) {
      return ((OffsetDateTime) v).toInstant().toEpochMilli();
    }
    if (v instanceof ZonedDateTime) {
      return ((ZonedDateTime) v).toInstant().toEpochMilli();
    }
    if (v instanceof java.time.LocalDateTime) {
      return ((java.time.LocalDateTime) v).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
    return Instant.parse(v.toString().replace(' ', 'T')).toEpochMilli();
  }

  private static String env(String key, String dflt)
  {
    final String v = System.getenv(key);
    return v == null || v.isEmpty() ? dflt : v;
  }

  private TrinoIngestion() {}
}
