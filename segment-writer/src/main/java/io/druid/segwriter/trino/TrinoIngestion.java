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
import com.google.common.io.Files;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segwriter.DruidSegmentWriter;
import io.druid.segwriter.Json;
import io.druid.segwriter.SegmentIngestSpec;
import io.druid.segwriter.SegmentIngestor;
import io.druid.segwriter.SegmentSpec;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
  // rows read so far, bumped as they are read (totalRows only moves when a whole chunk finishes, which is far too
  // coarse to tell "slow" from "stopped"). The heartbeat's read rate and the hang watchdog both key off this.
  private static final AtomicLong READ_ROWS = new AtomicLong();
  // rows PERSISTED so far (bumped as each shard's v9+lucene write completes). Paired with READ_ROWS so the heartbeat
  // can show read vs write rate separately: when read outruns write the shard backlog is growing (persist-bound,
  // heading for inflight backpressure); when they track, persist is keeping up. The gap is the tuning signal.
  private static final AtomicLong PERSISTED_ROWS = new AtomicLong();

  // live per-stream progress for the heartbeat: chunk start -> {rows read, shards handed off}. The reader is the
  // critical path, but it only prints at a shard boundary — every maxRows rows, ~40s apart — and those lines are
  // then buried under the persist pool's IndexMergerV9 INFO chatter (measured 28 merger lines per reader line). So
  // between shard boundaries there is no sign a stream is even alive. Written only by the owning reader (every 16k
  // rows, so the shared-map write is rare) and read approximately by the heartbeat; entries are removed on chunk end.
  private static final java.util.concurrent.ConcurrentHashMap<String, long[]> STREAM_PROGRESS =
      new java.util.concurrent.ConcurrentHashMap<>();

  private static final int PROGRESS_MASK = 0x3FFF;   // update every 16384 rows

  private static long ms(long nanos) { return nanos / 1_000_000L; }

  /** Per-second rate over the last tick. First tick (prev < 0) has no prior sample, so fall back to the cumulative
   *  average over the whole elapsed time. */
  private static long ratePerSec(long cur, long prev, long elapsedS, int tickS)
  {
    if (prev < 0) {
      return elapsedS > 0 ? cur / elapsedS : 0;
    }
    return (cur - prev) / tickS;
  }

  // Whether to push the time-sort down to Trino (ORDER BY). true (default): Trino sorts -> rows arrive presorted ->
  // the OnheapIncrementalIndex APPENDS (fast). false: NO ORDER BY -> Trino just streams a partition-pruned scan
  // (near-zero memory, no OOM on huge windows) and the index sorts on-heap per shard (slower but Trino-safe).
  // Set false for the row-dense windows whose ORDER BY (sorting rows that carry the big `raw` payload) OOMs Trino.
  private static boolean ORDER_BY = true;
  // benchmark: drain the ResultSet only (no segment build) to isolate Trino fetch throughput (default vs spooling)
  private static final boolean FETCH_ONLY = "true".equalsIgnoreCase(System.getenv("FETCH_ONLY"));

  public static void main(String[] args) throws Exception
  {
    ORDER_BY = Boolean.parseBoolean(env("TRINO_ORDER_BY", "true"));
    // presorted APPEND is ONLY valid when rows arrive time-sorted (ORDER BY). Force the property to match ORDER_BY
    // (set before OnheapIncrementalIndex is first loaded) — do NOT merely leave it, or a stale
    // -Ddruid.incrementalIndex.presortedAppend=true JVM arg would put the index in append mode over UNSORTED input,
    // producing a segment with a non-monotonic __time (inverted interval -> "end must be >= start" at query time).
    System.setProperty("druid.incrementalIndex.presortedAppend", String.valueOf(ORDER_BY));
    // INDEX_SORT=list -> sort-once-at-persist (append into an insertion-order map, TimSort at persist) instead of
    // the per-row TreeMap insert. Only for the unsorted (no ORDER BY) path; a benchmark toggle.
    if (!ORDER_BY && "list".equalsIgnoreCase(System.getenv("INDEX_SORT"))) {
      System.setProperty("druid.incrementalIndex.sortOnPersist", "true");
      System.out.println("trino-ingestion: index sort = list (sort-on-persist)");
    }
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
    // STREAMS = concurrent chunk queries = Trino-side concurrency. Each stream drains its ResultSet CONTINUOUSLY
    // (it hands each filled index to persistPool instead of writing it inline), so a stream never leaves its query
    // stalled on a full output buffer. WORKERS is the old name for this knob — honoured so existing manifests keep
    // their concurrency, but it no longer bounds persist (PERSIST_THREADS) nor live indexes (MAX_INFLIGHT_PERSIST).
    final int streams = Integer.parseInt(env("STREAMS", env("WORKERS", "2")));
    final int persistThreads = Integer.parseInt(env("PERSIST_THREADS", String.valueOf(streams)));
    // live on-heap indexes = STREAMS (accumulating) + MAX_INFLIGHT_PERSIST (queued/persisting). Raising this trades
    // heap for the reader's freedom to run ahead of a slow persist; when it is exhausted the reader blocks (and says so).
    final int maxInflight = Integer.parseInt(env("MAX_INFLIGHT_PERSIST", String.valueOf(streams)));
    final int pushThreads = Integer.parseInt(env("PUSH_THREADS", String.valueOf(streams)));

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
    // opt-in Trino spooling protocol (needs trino-jdbc 466+): e.g. "json+zstd" or "arrow" — workers write result
    // pages to spool storage (S3) that the client reads directly (compressed, parallel, bypassing the coordinator).
    final String encoding = System.getenv("TRINO_ENCODING");
    if (encoding != null && !encoding.isEmpty()) {
      props.setProperty("encoding", encoding);
    }
    // Whether spooling is actually ON takes THREE things — server enabled, a 466+ driver, and TRINO_ENCODING — and
    // a run missing any of them looks exactly like one that has them all. The pom's DEFAULT trino-jdbc is 435, which
    // cannot spool at all (466+ needs a Java 22+ build: -Dtrino.jdbc.version=476), and no build script overrides it,
    // so the driver in a given image is invisible after the fact. State both client-side facts up front and reject
    // the combination that cannot work, rather than silently running the plain protocol and calling it spooling.
    final java.sql.Driver driver = DriverManager.getDriver(jdbcUrl);
    final int driverMajor = driver.getMajorVersion();
    final boolean driverCanSpool = driverMajor >= 466;
    if (encoding != null && !encoding.isEmpty() && !driverCanSpool) {
      throw new IllegalStateException(
          "TRINO_ENCODING=" + encoding + " needs trino-jdbc 466+, but this build has " + driverMajor
          + " — rebuild with -Dtrino.jdbc.version=476 (needs a Java 22+ build/runtime)"
      );
    }
    System.out.println("trino-ingestion: trino-jdbc=" + driverMajor + "." + driver.getMinorVersion()
                       + " spooling=" + (encoding == null || encoding.isEmpty()
                                         ? "OFF (TRINO_ENCODING unset" + (driverCanSpool ? "" : "; driver cannot spool")
                                           + ")"
                                         : "requested (encoding=" + encoding + ")"));

    final String version = new DateTime().toString();   // one version for the whole run; > the HOUR segments' version
    final DataSegmentPusher pusher = SegmentIngestor.pusher(spec);

    // Chunk list. Default: split [start, end) into fixed CHUNK_HOURS windows (each = one parallel task = one
    // segment interval). Override: CHUNKS_FILE = a file with one "yyyy-MM-dd HH:mm:ss" (UTC) chunk-START per line
    // -> process EXACTLY those windows (each CHUNK_HOURS wide). Used to re-run only the windows a prior run missed
    // (e.g. chunks whose Trino query was killed by a cluster OOM) without rebuilding the whole range.
    final long startMs = start.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    final long endMs = end.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    final long chunkMs = chunkHours * 3_600_000L;
    final List<long[]> chunks = new ArrayList<>();
    final String chunksFile = System.getenv("CHUNKS_FILE");
    if (chunksFile != null && !chunksFile.isEmpty()) {
      for (String line : java.nio.file.Files.readAllLines(Paths.get(chunksFile))) {
        final String s = line.trim();
        if (s.isEmpty() || s.startsWith("#")) {
          continue;
        }
        final long c0 = ZonedDateTime.of(java.time.LocalDateTime.parse(s.replace(' ', 'T')), ZoneOffset.UTC)
                                     .toInstant().toEpochMilli();
        chunks.add(new long[]{c0, c0 + chunkMs, 0});   // [2] = expected row count, filled by LPT (0 = unknown)
      }
      System.out.println("trino-ingestion: CHUNKS_FILE=" + chunksFile + " -> " + chunks.size() + " explicit window(s)");
    } else {
      for (long c0 = startMs; c0 < endMs; c0 += chunkMs) {
        chunks.add(new long[]{c0, Math.min(c0 + chunkMs, endMs), 0});
      }
    }
    // LPT scheduling: submit the heaviest chunks FIRST so a single dense window can't become the idle-tail (one worker
    // grinding it while the rest sit idle). One grouped count maps each grid bucket -> row count; sort chunks densest-
    // first. Only for the grid case (CHUNKS_FILE windows are arbitrary/non-grid); LPT=false disables; failure falls
    // back to chronological order.
    if ((chunksFile == null || chunksFile.isEmpty()) && chunks.size() > 1
        && !"false".equalsIgnoreCase(System.getenv().getOrDefault("LPT", "true"))) {
      orderByRowsDesc(chunks, startMs, chunkMs, jdbcUrl, props, table, tsCol,
                      TS_FMT.format(Instant.ofEpochMilli(startMs)), TS_FMT.format(Instant.ofEpochMilli(endMs)));
    }
    final int nChunks = chunks.size();

    System.out.println("trino-ingestion: url=" + jdbcUrl + " table=" + table + " tsCol=" + tsCol
                       + " range=[" + start + "," + end + ") chunkHours=" + chunkHours + " chunks=" + nChunks
                       + " streams=" + streams + " persistThreads=" + persistThreads + " maxInflight=" + maxInflight
                       + " maxRows=" + maxRows + " orderBy=" + ORDER_BY + " version=" + version);

    final AtomicInteger totalSegs = new AtomicInteger();
    final AtomicLong totalRows = new AtomicLong();
    final AtomicInteger doneChunks = new AtomicInteger();
    final long wallStart = System.nanoTime();

    // heap/progress heartbeat: the per-chunk line only fires when a whole chunk finishes, so during a long dense
    // chunk there is no signal. Print JVM heap + running throughput every 15s so heap pressure is visible without
    // shelling into the pod. Reads READ_ROWS (bumped as rows are read, not at chunk end) so a stall shows up live.
    //
    // Doubles as the hang watchdog: if READ_ROWS does not move for STALL_DUMP_S, dump every thread's stack ONCE.
    // A hang here has so far left nothing to diagnose (the pod shows no progress and no error); the dump says
    // whether the readers sit in rs.next()/socket-read (a Trino-side stall), in inflight.acquire() (persist can't
    // keep up), or somewhere else entirely.
    final long stallDumpS = Long.parseLong(env("STALL_DUMP_S", "300"));
    final int tickS = 15;
    final Thread heartbeat = new Thread(() -> {
      final Runtime rt = Runtime.getRuntime();
      long lastRows = -1;
      long lastPersisted = -1;
      long stalledS = 0;
      boolean dumped = false;
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(tickS * 1000L);
        }
        catch (InterruptedException e) {
          return;
        }
        final long usedMb = (rt.totalMemory() - rt.freeMemory()) >> 20;
        final long maxMb = rt.maxMemory() >> 20;
        final long elapsedS = (System.nanoTime() - wallStart) / 1_000_000_000L;
        final long rows = READ_ROWS.get();
        final long persisted = PERSISTED_ROWS.get();
        // Rates are per-TICK (rows this 15s / 15s), not cumulative — cumulative hides a slow-down after a fast start,
        // and the whole point of splitting read from write is to catch the moment they diverge. read = rows pulled
        // from Trino; write = rows whose v9+lucene shard finished. read >> write for long = shard backlog growing
        // = persist-bound, heading for inflight backpressure. read is also the hang-watchdog signal (write can
        // legitimately sit at 0 while a big shard is mid-persist; read going flat is the stall).
        final long readRate = ratePerSec(rows, lastRows, elapsedS, tickS);
        final long writeRate = ratePerSec(persisted, lastPersisted, elapsedS, tickS);
        // NOTE: read/persisted are live (per-row/per-shard) while `chunks`/`segs` only move when a whole CHUNK
        // finishes, so a run with long chunks shows huge row counts against a small seg count. That is the counters
        // disagreeing, not lost work — the per-stream lines below show where the uncounted rows actually are.
        final StringBuilder sb = new StringBuilder();
        sb.append("[heartbeat] ").append(elapsedS).append("s heap=").append(usedMb).append('/').append(maxMb)
          .append("MB chunks=").append(doneChunks.get()).append('/').append(nChunks)
          .append(" segs=").append(totalSegs.get())
          .append(" read=").append(rows).append(" (").append(readRate).append("/s)")
          .append(" write=").append(persisted).append(" (").append(writeRate).append("/s)");
        for (Map.Entry<String, long[]> e : STREAM_PROGRESS.entrySet()) {
          final long[] p = e.getValue();
          sb.append("\n  [stream ").append(e.getKey()).append("] ").append(p[0]).append(" rows read, ")
            .append(p[1]).append(" shard(s) handed off");
        }
        System.out.println(sb);
        if (rows == lastRows && doneChunks.get() < nChunks) {
          stalledS += tickS;
          if (stalledS >= stallDumpS && !dumped) {
            dumped = true;
            dumpThreads(stalledS);
          }
        } else {
          stalledS = 0;
          dumped = false;
        }
        lastRows = rows;
        lastPersisted = persisted;
      }
    }, "heartbeat");
    heartbeat.setDaemon(true);
    heartbeat.start();

    // async push: workers persist (fetch+build+lucene, CPU-bound) and hand the finished v9 dir to a separate push
    // pool, then move on to the next shard/chunk -> the (slow, seaweed-bound) S3 upload of shard N overlaps the
    // persist of shard N+1. Bounded queue + CallerRunsPolicy: if pushes fall behind, the worker runs the push
    // inline (backpressure) so persisted-but-unpushed tmp dirs can't pile up and fill the pod's disk.
    final ThreadPoolExecutor pushPool = new ThreadPoolExecutor(
        pushThreads, pushThreads, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(Math.max(1, streams)), new ThreadPoolExecutor.CallerRunsPolicy());
    final List<Future<?>> pushFutures = Collections.synchronizedList(new ArrayList<>());

    // persist pool: the (CPU-bound) v9 columns + lucene write for a filled index, off the reader thread. This is
    // the whole point of the split — a reader that persists inline goes quiet for the entire write (measured at
    // ~1/3 of worker time), leaving its Trino query parked on a full output buffer for minutes at a time.
    final ExecutorService persistPool = Executors.newFixedThreadPool(persistThreads);
    final Semaphore inflight = new Semaphore(maxInflight);

    // across-chunk MT: STREAMS readers each run whole chunks (own Trino connection).
    final ExecutorService pool = Executors.newFixedThreadPool(streams);
    final List<Future<?>> futures = new ArrayList<>();
    for (final long[] c : chunks) {
      futures.add(pool.submit(() -> {
        processChunk(c[0], c[1], c[2], spec, jdbcUrl, props, table, colList, cols, tsCol, maxRows, version,
                     pusher, persistPool, inflight, pushPool, pushFutures, totalSegs, totalRows, doneChunks, nChunks);
        return null;
      }));
    }
    pool.shutdown();
    for (Future<?> f : futures) {
      f.get();   // surface any chunk (read/persist) failure (aborts the run)
    }
    persistPool.shutdown();
    persistPool.awaitTermination(1, TimeUnit.MINUTES);
    pushPool.shutdown();
    for (Future<?> f : pushFutures) {
      f.get();   // await + surface any async push failure
    }
    pushPool.awaitTermination(1, TimeUnit.MINUTES);

    final long wallMs = ms(System.nanoTime() - wallStart);
    final long fetchMs = ms(FETCH_NANOS.get());
    final long persistMs = ms(DruidSegmentWriter.Prof.persistNanos.get());
    final long pushMs = ms(DruidSegmentWriter.Prof.pushNanos.get());
    System.out.println("trino-ingestion: DONE. built " + totalSegs.get() + " segment(s), " + totalRows.get()
                       + " rows in " + wallMs + "ms ("
                       + (totalRows.get() * 1000L / Math.max(1, wallMs)) + " rows/s, " + streams + " streams)");
    // reader-thread phases (fetch = next + deser) run on the STREAMS reader threads; persist on PERSIST_THREADS;
    // push on PUSH_THREADS — each summed across its pool, so the three overlap and total > wall. index.add is no
    // longer timed: it was derived as accumulate-minus-fetch, but the read/persist split stopped populating
    // accumulateNanos, and per-row nanoTime over billions of rows would cost more than the number is worth.
    System.out.println("  AGGREGATE across threads (ms, summed over pools so > wall): fetch=" + fetchMs
                       + " [next=" + ms(NEXT_NANOS.get()) + " deser=" + ms(DESER_NANOS.get()) + "]"
                       + " persist(cols+lucene)=" + persistMs + " s3-push=" + pushMs);
  }

  private static final DateTimeFormatter TS_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

  private static final int MAX_RETRIES = Integer.parseInt(System.getenv().getOrDefault("CHUNK_MAX_RETRIES", "6"));

  /**
   * Process one time-chunk, retrying on a TRANSIENT Trino failure (chiefly "cluster is out of memory" — the
   * concurrent ORDER BY sorts can OOM the Trino cluster; Trino itself says "try again in a few minutes"). Each
   * attempt is idempotent: same version + shard numbers -> re-pushed objects overwrite; global counters are only
   * bumped on the ONE successful attempt (a failed attempt accumulates into locals that are discarded).
   */
  private static void processChunk(
      long c0, long c1, long expectedRows, SegmentIngestSpec spec, String jdbcUrl, Properties props, String table,
      String colList, List<String> cols, String tsCol, int maxRows, String version,
      DataSegmentPusher pusher, ExecutorService persistPool, Semaphore inflight,
      ExecutorService pushPool, List<Future<?>> pushFutures,
      AtomicInteger totalSegs, AtomicLong totalRows, AtomicInteger doneChunks, int nChunks
  ) throws Exception
  {
    final String c0s = TS_FMT.format(Instant.ofEpochMilli(c0));
    for (int attempt = 1; ; attempt++) {
      try {
        processChunkOnce(c0, c1, expectedRows, spec, jdbcUrl, props, table, colList, cols, tsCol, maxRows, version,
                         pusher, persistPool, inflight, pushPool, pushFutures, totalSegs, totalRows, doneChunks,
                         nChunks);
        return;
      }
      catch (Exception e) {
        if (attempt > MAX_RETRIES || !isTransientTrino(e)) {
          throw e;
        }
        final long backoffMs = 60_000L * attempt;   // Trino asked to wait "a few minutes"; linear backoff
        System.out.println("[chunk " + c0s + "] transient Trino failure (attempt " + attempt + "/" + MAX_RETRIES
                           + "), retrying in " + (backoffMs / 1000) + "s: " + rootMessage(e));
        Thread.sleep(backoffMs);
      }
    }
  }

  /**
   * Reorder {@code chunks} in place, densest-first (LPT), from one grouped count over the whole range. Each grid
   * bucket b = floor((rowMs - startMs) / chunkMs) corresponds to chunk index b, so the count array indexes chunks
   * directly. Empty windows (count 0) fall to the end and run last. Best-effort: any failure leaves the order intact.
   */
  private static void orderByRowsDesc(
      List<long[]> chunks, long startMs, long chunkMs, String jdbcUrl, Properties props, String table, String tsCol,
      String startLit, String endLit
  )
  {
    final long[] counts = new long[chunks.size()];
    final String sql = "SELECT CAST(FLOOR((to_unixtime(" + tsCol + ") * 1000 - " + startMs + ") / " + chunkMs
                       + ") AS BIGINT) b, count(*) c FROM " + table
                       + " WHERE " + tsCol + " >= TIMESTAMP '" + startLit + " UTC'"
                       + " AND " + tsCol + " < TIMESTAMP '" + endLit + " UTC' GROUP BY 1";
    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(sql)) {
      while (rs.next()) {
        final long b = rs.getLong(1);
        if (b >= 0 && b < counts.length) {
          counts[(int) b] = rs.getLong(2);
        }
      }
    }
    catch (Exception e) {
      System.out.println("trino-ingestion: LPT count failed (" + e + ") — keeping chronological order");
      return;
    }
    chunks.sort((x, y) -> Long.compare(counts[(int) ((y[0] - startMs) / chunkMs)],
                                       counts[(int) ((x[0] - startMs) / chunkMs)]));
    for (long[] c : chunks) {   // stash each chunk's row count in [2] so processChunk can log shard N/total
      final int b = (int) ((c[0] - startMs) / chunkMs);
      if (b >= 0 && b < counts.length) {
        c[2] = counts[b];
      }
    }
    long tot = 0;
    long max = 0;
    for (long c : counts) {
      tot += c;
      max = Math.max(max, c);
    }
    System.out.println("trino-ingestion: LPT — densest chunk " + max + " rows, " + tot
                       + " total across " + counts.length + " chunk(s); processing densest-first");
  }

  /**
   * A single attempt: own Trino connection -> query -> read rows into an index, handing each full index to
   * persistPool (which pushes) and immediately continuing to read -> await this attempt's persists.
   *
   * <p>The reader never writes a segment itself, so the ResultSet is drained continuously; the only thing that can
   * pause it is {@code inflight} (the on-heap index budget), which logs when it does.
   */
  private static void processChunkOnce(
      long c0, long c1, long expectedRows, SegmentIngestSpec spec, String jdbcUrl, Properties props, String table,
      String colList, List<String> cols, String tsCol, int maxRows, String version,
      DataSegmentPusher pusher, ExecutorService persistPool, Semaphore inflight,
      ExecutorService pushPool, List<Future<?>> pushFutures,
      AtomicInteger totalSegs, AtomicLong totalRows, AtomicInteger doneChunks, int nChunks
  ) throws Exception
  {
    final String c0s = TS_FMT.format(Instant.ofEpochMilli(c0));
    // expected shard count from the LPT row count (0 -> unknown, e.g. CHUNKS_FILE runs); used only for the log below.
    final int totalShards = expectedRows > 0 ? (int) ((expectedRows + maxRows - 1) / maxRows) : -1;
    final String c1s = TS_FMT.format(Instant.ofEpochMilli(c1));
    final String sql = "SELECT " + colList + " FROM " + table
                       + " WHERE " + tsCol + " >= TIMESTAMP '" + c0s + " UTC'"
                       + " AND " + tsCol + " < TIMESTAMP '" + c1s + " UTC'"
                       + (ORDER_BY ? " ORDER BY " + tsCol + " ASC" : "");   // no ORDER BY -> Trino won't sort (OOM-safe)
    final Interval iv = new Interval(c0, c1);
    final SegmentSpec segSpec = SegmentIngestor.segmentSpec(spec);
    // local tallies: only folded into the global counters on success, so a retried attempt can't double-count
    int localSegs = 0;
    long chunkRows = 0;
    // this attempt's persists. Awaited before the attempt returns (success: to surface failures; failure: so no
    // retry can re-persist a shard number that an in-flight persist of the dead attempt is still pushing).
    final List<Future<?>> attemptPersists = new ArrayList<>();
    final long[] progress = new long[2];   // {rows read, shards handed off} — published to the heartbeat
    STREAM_PROGRESS.put(c0s, progress);
    try {
      try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
           Statement stmt = conn.createStatement()) {
        stmt.setFetchSize(10_000);
        try (ResultSet rs = stmt.executeQuery(sql)) {
          final Iterator<Map<String, Object>> rows = rowIterator(rs, cols, tsCol);
          if (FETCH_ONLY) {
            // benchmark mode: drain the ResultSet (reads every column incl. the big `raw` payload) with NO
            // index/persist/push, to isolate the Trino fetch throughput (default vs spooling protocol).
            final long t0 = System.nanoTime();
            long n = 0;
            while (rows.hasNext()) {
              rows.next();
              n++;
              READ_ROWS.incrementAndGet();
            }
            final long ms = (System.nanoTime() - t0) / 1_000_000L;
            System.out.println("[fetch-only " + c0s + "] " + n + " rows in " + ms + "ms ("
                               + (n * 1000L / Math.max(1, ms)) + " rows/s)");
            totalRows.addAndGet(n);
            doneChunks.incrementAndGet();
            return;
          }
          int shard = 0;
          int n = 0;
          System.out.println("[chunk " + c0s + " shard 1/" + (totalShards > 0 ? totalShards : "?") + "] reading");
          IncrementalIndex index = DruidSegmentWriter.newIndex(segSpec, iv);
          boolean owned = true;   // `index` is ours to close until it is handed to persistPool
          try {
            while (rows.hasNext()) {
              DruidSegmentWriter.addRow(index, segSpec, rows.next());
              READ_ROWS.incrementAndGet();
              if (++n < maxRows) {
                if ((n & PROGRESS_MASK) == 0) {
                  progress[0] = chunkRows + n;
                }
                continue;
              }
              // shard full: hand it off and keep reading into a fresh index — the ResultSet never goes quiet.
              // submitPersist takes ownership of the index only by returning; if it throws, `owned` stays true
              // and the finally below closes it.
              final Future<?> persist =
                  submitPersist(index, spec, iv, version, shard, c0s, persistPool, inflight, pusher, pushPool,
                                pushFutures);
              owned = false;
              attemptPersists.add(persist);
              localSegs++;
              chunkRows += n;
              shard++;
              n = 0;
              progress[0] = chunkRows;
              progress[1] = shard;
              System.out.println("[chunk " + c0s + " shard " + (shard + 1) + "/"
                                 + (totalShards > 0 ? totalShards : "?") + "] reading ("
                                 + chunkRows + " rows so far, persisting shard " + shard + " in background)");
              index = DruidSegmentWriter.newIndex(segSpec, iv);
              owned = true;
            }
            if (n > 0) {
              final Future<?> persist =
                  submitPersist(index, spec, iv, version, shard, c0s, persistPool, inflight, pusher, pushPool,
                                pushFutures);
              owned = false;
              attemptPersists.add(persist);
              localSegs++;
              chunkRows += n;
              shard++;
            }
          }
          finally {
            if (owned) {
              closeQuietly(index);   // trailing empty index, or a read that threw mid-shard
            }
          }
        }
      }
      // connection closed: nothing is waiting on Trino while we drain the tail of this chunk's persists.
      for (Future<?> f : attemptPersists) {
        f.get();
      }
      totalSegs.addAndGet(localSegs);
      totalRows.addAndGet(chunkRows);
      final int done = doneChunks.incrementAndGet();
      System.out.println("[chunk " + done + "/" + nChunks + " " + c0s + "] " + localSegs + " shard(s), " + chunkRows
                         + " rows (running: " + totalSegs.get() + " segs, " + totalRows.get() + " rows)");
    }
    catch (Exception e) {
      awaitQuietly(attemptPersists);
      throw e;
    }
    finally {
      STREAM_PROGRESS.remove(c0s);
    }
  }

  /**
   * Hand a filled index to the persist pool (which persists, then hands the dir to the push pool) and return at
   * once, so the caller can go back to reading. Blocks only when the on-heap index budget is exhausted.
   */
  private static Future<?> submitPersist(
      IncrementalIndex index, SegmentIngestSpec spec, Interval iv, String version, int shard, String c0s,
      ExecutorService persistPool, Semaphore inflight, DataSegmentPusher pusher,
      ExecutorService pushPool, List<Future<?>> pushFutures
  ) throws InterruptedException
  {
    // reader-side backpressure: bounds live indexes. Blocking here re-stalls the Trino read (the very thing this
    // split exists to prevent), so say so — it means persist can't keep up and PERSIST_THREADS/MAX_INFLIGHT_PERSIST
    // (or a smaller maxRowsPerSegment) needs a look.
    if (!inflight.tryAcquire(30, TimeUnit.SECONDS)) {
      System.out.println("[chunk " + c0s + "] persist backpressure: reader paused (Trino stream idle) waiting for an"
                         + " in-flight persist — consider raising PERSIST_THREADS/MAX_INFLIGHT_PERSIST or lowering"
                         + " maxRowsPerSegment");
      inflight.acquire();
    }
    boolean submitted = false;
    try {
      final Future<?> f = persistPool.submit(() -> {
        final File tmp = Files.createTempDir();
        boolean handedOff = false;
        try {
          final DruidSegmentWriter.Persisted p =   // CPU-bound (v9 columns + lucene); closes the index
              SegmentIngestor.persistIndex(spec, index, iv, version, shard, 2, tmp);
          PERSISTED_ROWS.addAndGet(p.template.getNumRows());
          System.out.println("  persisted " + p.template.getIdentifier() + " rows=" + p.template.getNumRows()
                             + " -> push");
          pushFutures.add(pushPool.submit(() -> {
            try {
              final long tPush = System.nanoTime();
              final DataSegment seg = pusher.push(p.dir, p.template);
              DruidSegmentWriter.Prof.pushNanos.addAndGet(System.nanoTime() - tPush);
              System.out.println("  pushed " + seg.getIdentifier() + " size=" + seg.getSize());
              return null;
            }
            finally {
              FileUtils.deleteQuietly(tmp);   // free scratch only after the push has read it
            }
          }));
          handedOff = true;
          return null;
        }
        finally {
          if (!handedOff) {
            FileUtils.deleteQuietly(tmp);   // persist failed before hand-off: clean up here
          }
          inflight.release();
        }
      });
      submitted = true;
      return f;
    }
    finally {
      if (!submitted) {
        inflight.release();
      }
    }
  }

  /** Await tasks we are abandoning (their failures are noise next to the one we are already throwing). */
  private static void awaitQuietly(List<Future<?>> futures)
  {
    for (Future<?> f : futures) {
      try {
        f.get();
      }
      catch (Exception ignored) {
        // fall through: we only need it to be finished, not successful
      }
    }
  }

  private static void closeQuietly(IncrementalIndex index)
  {
    try {
      index.close();
    }
    catch (Exception ignored) {
      // best-effort
    }
  }

  /** Dump every thread's stack — the hang watchdog's payload (see the heartbeat). */
  private static void dumpThreads(long stalledS)
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("[watchdog] no rows read for ").append(stalledS).append("s — thread dump follows\n");
    for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
      final Thread t = e.getKey();
      sb.append("\n\"").append(t.getName()).append("\" ").append(t.getState()).append('\n');
      for (StackTraceElement f : e.getValue()) {
        sb.append("\tat ").append(f).append('\n');
      }
    }
    System.out.println(sb);
  }

  /** True if the failure is a transient Trino cluster condition worth retrying (out-of-memory / try again). */
  private static boolean isTransientTrino(Throwable e)
  {
    for (Throwable t = e; t != null; t = t.getCause()) {
      final String m = t.getMessage();
      if (m != null) {
        final String lm = m.toLowerCase(java.util.Locale.ROOT);
        if (lm.contains("out of memory") || lm.contains("try again")
            || lm.contains("exceeded") && lm.contains("memory")) {
          return true;
        }
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return false;
  }

  private static String rootMessage(Throwable e)
  {
    Throwable t = e;
    while (t.getCause() != null && t.getCause() != t) {
      t = t.getCause();
    }
    return t.getMessage();
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
