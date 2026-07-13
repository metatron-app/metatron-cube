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
import io.druid.common.utils.Sequences;
import io.druid.guice.annotations.Json;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
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

    final Map<String, Object> out = Maps.newLinkedHashMap();
    out.put("dataSource", dataSource);
    // Distinct keys — the core; its failure IS the resolve's failure (nothing useful without them).
    if (keyColumns != null && !keyColumns.isEmpty()) {
      try {
        // The caller speaks in SOURCE column names; the datasource's source time column (from the timeColumns config)
        // is Druid's __time, so translate it for the internal query. The response echoes the caller's names as-is.
        final List<String> queryColumns = toInternal(keyColumns, catalog.getTimeColumns().get(dataSource));
        final List<Object[]> rows = run(streamQuery(dataSource, intervals, filter, queryColumns, limit), remoteAddr);
        final boolean scalar = keyColumns.size() == 1;   // single key -> flat values; multiple -> tuples
        final List<Object> values = Lists.newArrayListWithCapacity(rows.size());
        for (Object[] row : rows) {
          values.add(scalar ? row[0] : java.util.Arrays.asList(row));
        }
        out.put("key", keyObj);
        out.put("count", values.size());
        out.put("capped", values.size() >= limit);   // hit the cap -> connector should skip pushdown
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
