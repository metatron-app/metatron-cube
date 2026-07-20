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

package io.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yahoo.sketches.quantiles.ItemsSketch;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Json;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.sketch.QuantileOperation;
import io.druid.query.sketch.TypedSketch;
import io.druid.server.QueryManager;
import io.druid.server.QueryStats;
import io.druid.server.RequestLogLine;
import io.druid.server.coordination.StandaloneCatalogConfig;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.AuthConfig;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The "resolve" endpoint the Trino connector uses Druid as a secondary INDEX: given a filter (typically a lucene
 * query on {@code raw}) it returns the DISTINCT values of a key column (e.g. {@code source_sha256}) for injection as
 * a {@code key IN (...)} predicate on the source table. To also prune the source table's time partitions, ask for the
 * source time column alongside the key ({@code "key":["source_sha256","timestamp_trigger"]}): the source is 1:1 on
 * (source_sha256, timestamp_trigger), so each returned pair carries that key's exact time — no separate time-bounds
 * aggregation (a second full scan) needed. The caller uses SOURCE column names throughout; the server maps the
 * datasource's source time column (per the timeColumns config) to Druid's {@code __time} internally.
 *
 * <pre>POST /druid/v2/resolve
 * { "dataSource":"atom_credential",
 *   "filter":{"type":"lucene.query","field":"raw","expression":"naver*"},
 *   "key":"source_sha256",          // or ["source_sha256","timestamp_trigger"] for (key, time) tuples
 *   "interval":["2026-04-20T00:00:00Z","2026-07-10T00:00:00Z"],  // optional
 *   "limit":100000 }                // optional cap; capped=true if the distinct count reaches it
 * -> { "dataSource":..., "key":"source_sha256", "count":2046, "capped":false, "values":[...] }</pre>
 *
 * Distinct keys use a parallel select.stream with dedup done in the producers (context {@code dedup}): one lucene scan,
 * W-way parallel DISTINCT, keeps the fast lucene path and avoids the groupBy 500k merge cap.
 */
@Path("/druid/v2/resolve")
public class ResolveResource
{
  private static final io.druid.java.util.common.logger.Logger LOG =
      new io.druid.java.util.common.logger.Logger(ResolveResource.class);

  private static final int DEFAULT_LIMIT = 100_000;
  private static final long TIMEOUT_MS = 900_000L;
  private static final String ETERNITY = "1000-01-01/3000-01-01";

  private final QuerySegmentWalker walker;
  private final QueryManager queryManager;
  private final RequestLogger requestLogger;
  private final StandaloneCatalogConfig catalog;
  private final ObjectMapper mapper;

  @Inject
  public ResolveResource(
      QuerySegmentWalker walker,
      QueryManager queryManager,
      RequestLogger requestLogger,
      StandaloneCatalogConfig catalog,
      @Json ObjectMapper mapper
  )
  {
    this.walker = walker;
    this.queryManager = queryManager;
    this.requestLogger = requestLogger;
    this.catalog = catalog;
    this.mapper = mapper;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response resolve(Map<String, Object> request, @Context HttpServletRequest req)
  {
    // This endpoint is internal + network-gated and does no per-datasource authorization, so the response filter
    // (PreResponseAuthorizationCheckFilter) would WARN "Request did not have an authorization check performed" on
    // every call. Mark the request as authorization-checked to satisfy it.
    if (req != null) {
      req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    }
    final String remoteAddr = req == null ? "" : Strings.nullToEmpty(req.getRemoteAddr());
    // Dump the connector's request verbatim so we can see exactly what it sent (filter, key, limit, score) — pins
    // whether an over-fetch is the connector's `limit`/`score` or something the resolve endpoint derives.
    LOG.info("[resolve-req] from[%s] %s", remoteAddr, request);
    final String dataSource = (String) request.get("dataSource");
    if (dataSource == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.of("error", "dataSource is required")).build();
    }
    final Object filter = request.get("filter");
    final Object keyObj = request.get("key");
    final List<String> keyColumns = keyColumnsOf(keyObj);   // SOURCE column names, e.g. ["source_sha256","timestamp_trigger"]
    final int limit = request.get("limit") instanceof Number ? ((Number) request.get("limit")).intValue() : DEFAULT_LIMIT;
    final Object interval = request.get("interval");
    final List<String> intervals = Lists.newArrayList(intervalOf(interval));

    // Score mode: return the max relevance score per key (ranked), not just the distinct keys. Needs a lucene.query
    // filter to score against; the connector cannot do this itself since the score lives in the lucene scan.
    final boolean scored = asBool(request.get("score"));

    final Map<String, Object> out = Maps.newLinkedHashMap();
    out.put("dataSource", dataSource);
    // Distinct keys — the core; its failure IS the resolve's failure (nothing useful without them).
    if (keyColumns != null && !keyColumns.isEmpty()) {
      // The caller asks for the final top-`limit` only; the per-segment scan fan-out is an internal recall knob
      // (a key's best row must survive its segment's cutoff), NOT something the connector should reason about.
      final Object scoredFilter = scored ? withScoreField(filter, scanLimitFor(limit), null) : null;
      if (scored && scoredFilter == null) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ImmutableMap.of("error", "score requires a lucene.query filter")).build();
      }
      try {
        // The caller speaks in SOURCE column names; the datasource's source time column (from the timeColumns config)
        // is Druid's __time, so translate it for the internal query. The response echoes the caller's names as-is.
        final List<String> queryColumns = toInternal(keyColumns, catalog.getTimeColumns().get(dataSource));
        final List<Object> values;
        if (scored) {
          // Two-pass to avoid materializing every candidate row (the key columns — e.g. a 64-char file_sha256 — cost
          // far more than the score). PASS 1 (scoreThreshold): a score-only scan finds the global top-`limit` cutoff
          // score T. PASS 2: replay the same scan with minScore=T so only docs that can make the global top-N enter
          // the bitmap -> the select.stream cursor materializes ~limit rows, not the whole match set. scoreDedup then
          // ranks ~limit and keeps the max score per key (a no-op for a row-unique key like file_sha256+line_no, a
          // real collapse for a coarse key). Exact for row-unique keys; approximate for coarse (as before).
          final float threshold = scoreThreshold(dataSource, intervals, scoredFilter, limit, remoteAddr);
          final Object scoredFilter2 = threshold == Float.NEGATIVE_INFINITY
                                       ? scoredFilter   // fewer matches than `limit` -> no floor, take them all
                                       : withScoreField(filter, scanLimitFor(limit), threshold);
          final List<Object[]> rows = run(scoredStreamQuery(dataSource, intervals, scoredFilter2, queryColumns, limit), remoteAddr);
          values = scoreDedup(rows, queryColumns.size(), limit);
        } else {
          final List<Object[]> rows = run(streamQuery(dataSource, intervals, filter, queryColumns, limit), remoteAddr);
          final boolean scalar = keyColumns.size() == 1;   // single key -> flat values; multiple -> tuples
          values = Lists.newArrayListWithCapacity(rows.size());
          for (Object[] row : rows) {
            values.add(scalar ? row[0] : java.util.Arrays.asList(row));
          }
        }
        out.put("key", keyObj);
        if (scored) {
          out.put("scored", true);   // values are [keyColumns..., score] tuples, ordered by score desc
        }
        out.put("count", values.size());
        out.put("capped", values.size() >= limit);   // hit the cap -> distinct: skip pushdown; scored: top-`limit` keys
        out.put("values", values);
      }
      catch (Exception e) {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                       .entity(ImmutableMap.of("error", String.valueOf(e.getMessage()))).build();
      }
    }
    return Response.ok(out).build();
  }

  // Map the datasource's source time column -> "__time" for the internal query; other names pass through. "__time" is
  // also accepted directly (passes through). null timeCol (not configured) -> no translation.
  private static List<String> toInternal(List<String> columns, String timeCol)
  {
    if (timeCol == null) {
      return columns;
    }
    final List<String> out = Lists.newArrayListWithCapacity(columns.size());
    for (String c : columns) {
      out.add(timeCol.equals(c) ? "__time" : c);
    }
    return out;
  }

  @SuppressWarnings("unchecked")
  private static List<String> keyColumnsOf(Object key)
  {
    if (key instanceof String) {
      return Lists.newArrayList((String) key);
    }
    return key instanceof List ? (List<String>) key : null;
  }

  private Map<String, Object> streamQuery(String ds, List<String> intervals, Object filter, List<String> columns, int limit)
  {
    final Map<String, Object> q = Maps.newLinkedHashMap();
    q.put("queryType", "select.stream");
    q.put("dataSource", ds);
    q.put("intervals", intervals);
    if (filter != null) {
      q.put("filter", filter);
    }
    q.put("columns", columns);
    q.put("limitSpec", ImmutableMap.of("type", "default", "limit", limit));
    q.put("context", ImmutableMap.of("dedup", true, "timeout", TIMEOUT_MS, "queryId", newId()));
    return q;
  }

  // Score mode: stream each matching row's lucene score (attached under _score by the scoreField on the filter,
  // surfaced as a virtual column). The filter's per-segment `limit` keeps only the top-N rows by score per segment, so
  // the stream is bounded (~limit x segments rows) — no groupBy, no merge cap, whatever the key's cardinality. The
  // max-per-key collapse then happens in scoreDedup. NOT deduped in the engine (we need the score, not just the key).
  private Map<String, Object> scoredStreamQuery(String ds, List<String> intervals, Object filter, List<String> keyColumns, int limit)
  {
    final List<String> columns = Lists.newArrayList(keyColumns);
    columns.add("_score");   // appended per-row score
    final Map<String, Object> q = Maps.newLinkedHashMap();
    q.put("queryType", "select.stream");
    q.put("dataSource", ds);
    q.put("intervals", intervals);
    q.put("filter", filter);   // already carries scoreField=_score + per-segment `limit` (see withScoreField)
    q.put("virtualColumns", Lists.newArrayList(
        ImmutableMap.of("type", "$attachment", "outputName", "_score", "columnType", "FLOAT")
    ));
    q.put("columns", columns);
    // the filter's per-segment limit bounds the rows; keep the stream unbounded here and take the global top-N in Java.
    q.put("limitSpec", ImmutableMap.of("type", "default", "limit", Integer.MAX_VALUE));
    q.put("context", ImmutableMap.of("timeout", TIMEOUT_MS, "queryId", newId()));
    return q;
  }

  // PASS 1 of score mode: a score-only scan that returns the global top-`limit` cutoff score T (docs below T can't
  // make the final top-N, so PASS 2 skips materializing them). Projects only __time (a cheap resident long) + _score,
  // NOT the key columns, so it reads no expensive output columns — the scan still scores every match (~the same cost
  // as the old single pass minus the column materialization). row = [__time, score]. Returns NEGATIVE_INFINITY when
  // there are fewer than `limit` rows (no floor -> PASS 2 returns them all). T is exact for a row-unique key; for a
  // coarse key the row-level cutoff can admit slightly fewer distinct keys — within the pre-existing approximation.
  private float scoreThreshold(String ds, List<String> intervals, Object scoredFilter, int limit, String remoteAddr)
  {
    // PASS 1: find the floor score T = the scanN-th largest _score, via a SERVER-SIDE aggregation instead of streaming
    // every match's score up to here. A `timeseries` with a `count` + a QUANTILE `sketch` over the _score attachment
    // ingests scores in-engine (per segment, merged at the broker); only the count + a KB sketch flow back — no row
    // streaming. T is read as the scanN-th-largest quantile. Margin note: scanN = scanLimitFor(limit) (the 4x recall
    // knob), so PASS 2 admits the top ~scanN rows and scoreDedup trims to exactly `limit`, and T is biased low enough
    // that a key repeating across rows still leaves >= limit distinct keys. The sketch is APPROXIMATE, but because T
    // only needs to be <= the true limit-th key score (a low bias over-admits, harmlessly), the final top-N stays exact.
    final int scanN = scanLimitFor(limit);
    final List<Row> rows = run(scoreSketchQuery(ds, intervals, scoredFilter), remoteAddr);
    if (rows.isEmpty()) {
      return Float.NEGATIVE_INFINITY;
    }
    final Row row = rows.get(0);
    final Object cntRaw = row.getRaw("cnt");
    final long count = cntRaw instanceof Number ? ((Number) cntRaw).longValue() : 0L;
    final Object skRaw = row.getRaw("score_sketch");
    if (count <= scanN || !(skRaw instanceof TypedSketch)) {
      return Float.NEGATIVE_INFINITY;   // fewer matches than the margin (or no sketch) -> no floor, PASS 2 takes all
    }
    final ItemsSketch sketch = (ItemsSketch) ((TypedSketch) skRaw).value();
    // ascending sketch: getQuantile(f) = value at normalized rank f from the smallest, so 1 - scanN/count = scanN-th largest
    final Object t = QuantileOperation.QUANTILES.calculate(sketch, 1.0 - (double) scanN / count);
    return t instanceof Number ? ((Number) t).floatValue() : Float.NEGATIVE_INFINITY;
  }

  // PASS 1 query: aggregate the _score attachment server-side (count + QUANTILE sketch) — NO key columns, NO row
  // output. The lucene filter attaches _score during bitmap extraction; the $attachment VC surfaces it, and the
  // aggregation cursor reads it per matched row to feed the sketch.
  private Map<String, Object> scoreSketchQuery(String ds, List<String> intervals, Object filter)
  {
    final Map<String, Object> q = Maps.newLinkedHashMap();
    q.put("queryType", "timeseries");
    q.put("dataSource", ds);
    q.put("intervals", intervals);
    q.put("granularity", "all");
    q.put("filter", filter);
    q.put("virtualColumns", Lists.newArrayList(
        ImmutableMap.of("type", "$attachment", "outputName", "_score", "columnType", "FLOAT")
    ));
    q.put("aggregations", Lists.newArrayList(
        ImmutableMap.of("type", "count", "name", "cnt"),
        ImmutableMap.of("type", "sketch", "name", "score_sketch", "fieldName", "_score", "inputType", "float", "sketchOp", "QUANTILE")
    ));
    q.put("context", ImmutableMap.of("timeout", TIMEOUT_MS, "queryId", newId()));
    return q;
  }

  // Max score per key over the streamed (per-segment top-N) rows: rows are [keyColumns..., score]. A row-unique key
  // collapses to itself; a coarse key keeps its best-scoring occurrence. Rank score-desc, keep the top-`limit`, and
  // emit [keyColumns..., score] tuples.
  private static List<Object> scoreDedup(List<Object[]> rows, int keyLen, int limit)
  {
    final Map<List<Object>, Double> best = Maps.newHashMap();
    for (Object[] row : rows) {
      if (!(row[keyLen] instanceof Number)) {
        continue;   // no score attached (shouldn't happen for a matched row) — skip
      }
      final List<Object> key = java.util.Arrays.asList(java.util.Arrays.copyOf(row, keyLen));
      final double score = ((Number) row[keyLen]).doubleValue();
      best.merge(key, score, Math::max);
    }
    final List<Map.Entry<List<Object>, Double>> ranked = Lists.newArrayList(best.entrySet());
    ranked.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));   // score desc
    final List<Object> values = Lists.newArrayListWithCapacity(Math.min(ranked.size(), limit));
    for (int i = 0; i < ranked.size() && i < limit; i++) {
      final List<Object> tuple = Lists.newArrayList(ranked.get(i).getKey());
      tuple.add(ranked.get(i).getValue());
      values.add(tuple);
    }
    return values;
  }

  // Per-segment scan limit for score mode, derived from the caller's final top-K. Each segment returns its top
  // (K x fanout) rows by score so a global top-K key whose best row is crowded low in one segment still survives to
  // the merge; the fanout is a server-side recall knob, not exposed to the connector. Scoring cost is independent of
  // this (collectAllScored scores every match regardless — the limit only selects), so it only trades a few more
  // streamed rows for recall. Capped so a huge `limit` can't stream unbounded rows into the resolve resource.
  private static final int SCORE_FANOUT = 4;
  private static final int SCORE_SCAN_CAP = 200_000;

  private static int scanLimitFor(int topK)
  {
    return (int) Math.min((long) topK * SCORE_FANOUT, SCORE_SCAN_CAP);
  }

  private static boolean asBool(Object v)
  {
    return v instanceof Boolean ? (Boolean) v : "true".equalsIgnoreCase(String.valueOf(v));
  }

  // Copy the caller's filter with scoreField=_score (so the lucene scan attaches a per-doc score) and a per-segment
  // `limit` = top-N rows by score (which bounds the scored stream). When {@code minScore} is non-null, also inject it
  // as a relevance floor so only docs scoring >= minScore enter the bitmap (PASS 2 of the two-pass). Only a top-level
  // lucene.query is scorable here; anything else returns null (score mode then 400s).
  @SuppressWarnings("unchecked")
  private static Object withScoreField(Object filter, int limit, Float minScore)
  {
    if (filter instanceof Map && "lucene.query".equals(((Map<String, Object>) filter).get("type"))) {
      final Map<String, Object> copy = Maps.newLinkedHashMap((Map<String, Object>) filter);
      copy.put("scoreField", "_score");
      if (limit > 0) {
        copy.put("limit", limit);   // per-segment top-N by score -> bounds the stream
      }
      if (minScore != null) {
        copy.put("minScore", minScore);   // relevance floor -> gates which docs materialize (PASS 2)
      }
      return copy;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private <T> List<T> run(Map<String, Object> queryMap, String remoteAddr)
  {
    final Query<T> query = (Query<T>) mapper.convertValue(queryMap, Query.class);
    // Register with the QueryManager so it gets a SessionCache (queries run outside the normal QueryResource path
    // otherwise NPE on QueryWatcher.getSessionCache) + a cancellation handle; unregister when done.
    queryManager.register(query, SettableFuture.create(), null);
    final long startNs = System.nanoTime();
    final long startMs = System.currentTimeMillis();
    Throwable error = null;
    List<T> rows = null;
    try {
      rows = Sequences.toList(query.run(walker, Maps.newHashMap()));
      return rows;
    }
    catch (Throwable t) {
      error = t;
      throw t;
    }
    finally {
      queryManager.unregister(query, null);
      // Log to the RequestLogger like the normal query path, since these queries bypass QueryResource/QueryLifecycle.
      log(query, remoteAddr, startMs, startNs, rows == null ? -1 : rows.size(), error);
    }
  }

  private void log(Query<?> query, String remoteAddr, long startMs, long startNs, int rows, Throwable e)
  {
    try {
      final Map<String, Object> stats = Maps.newLinkedHashMap();
      stats.put("success", e == null);
      stats.put("query/time", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
      stats.put("query/rows", rows);
      stats.put("query/bytes", -1);
      if (e != null) {
        stats.put("exception", String.valueOf(e));
      }
      requestLogger.log(new RequestLogLine(new DateTime(startMs, DateTimeZone.UTC), remoteAddr, query, new QueryStats(stats)));
    }
    catch (Exception ignore) {
      // logging must never break the response
    }
  }

  private static String newId()
  {
    return "resolve-" + java.util.UUID.randomUUID();
  }

  private static String intervalOf(Object interval)
  {
    if (interval instanceof List<?> iv && iv.size() == 2) {
      return iv.get(0) + "/" + iv.get(1);
    }
    return interval instanceof String ? (String) interval : ETERNITY;
  }
}
