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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Json;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.server.QueryManager;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * The "resolve" endpoint the Trino connector uses Druid as a secondary INDEX: given a filter (typically a lucene
 * query on {@code raw}) it returns the DISTINCT values of a key column (e.g. {@code source_sha256}) for injection as
 * a {@code key IN (...)} predicate on the source table, and/or the {@code __time} span of the matches for iceberg
 * partition pruning. The connector sends intent; the server picks the query shape.
 *
 * <pre>POST /druid/v2/resolve
 * { "dataSource":"atom_credential",
 *   "filter":{"type":"lucene.query","field":"raw","expression":"naver*"},
 *   "key":"source_sha256",          // optional: distinct values of this column
 *   "interval":["2026-04-20T00:00:00Z","2026-07-10T00:00:00Z"],  // optional
 *   "limit":100000,                 // optional cap; capped=true if the distinct count reaches it
 *   "timeBounds":true }             // optional (default true): also return min/max __time
 * -> { "dataSource":..., "key":"source_sha256", "count":2046, "capped":false, "values":[...],
 *      "timeBounds":["2026-04-20T04:00:00.000Z","2026-07-09T00:00:00.000Z"] }</pre>
 *
 * Distinct keys use a parallel, non-scoring select.stream with per-segment dedup (context {@code dedup}); time bounds
 * use a timeseries {@code longMin}/{@code longMax} over {@code __time}. Both keep the fast lucene path and avoid the
 * groupBy 500k merge cap.
 */
@Path("/druid/v2/resolve")
public class ResolveResource
{
  private static final int DEFAULT_LIMIT = 100_000;
  private static final long TIMEOUT_MS = 900_000L;
  private static final String ETERNITY = "1000-01-01/3000-01-01";

  private final QuerySegmentWalker walker;
  private final QueryManager queryManager;
  private final ObjectMapper mapper;

  @Inject
  public ResolveResource(QuerySegmentWalker walker, QueryManager queryManager, @Json ObjectMapper mapper)
  {
    this.walker = walker;
    this.queryManager = queryManager;
    this.mapper = mapper;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response resolve(Map<String, Object> request)
  {
    final String dataSource = (String) request.get("dataSource");
    if (dataSource == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.of("error", "dataSource is required")).build();
    }
    final Object filter = request.get("filter");
    final Object keyObj = request.get("key");
    final List<String> keyColumns = keyColumnsOf(keyObj);   // "source_sha256" or ["source_sha256","__time"]
    final int limit = request.get("limit") instanceof Number ? ((Number) request.get("limit")).intValue() : DEFAULT_LIMIT;
    final boolean wantTimeBounds = Boolean.TRUE.equals(request.get("timeBounds"));   // opt-in
    final Object interval = request.get("interval");
    final List<String> intervals = Lists.newArrayList(intervalOf(interval));

    final Map<String, Object> out = Maps.newLinkedHashMap();
    out.put("dataSource", dataSource);
    // Distinct keys — the core; its failure IS the resolve's failure (nothing useful without them).
    if (keyColumns != null && !keyColumns.isEmpty()) {
      try {
        final List<Object[]> rows = run(streamQuery(dataSource, intervals, filter, keyColumns, limit));
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
    // Time bounds — best-effort, fully isolated: a failure only omits them (+ a note), never drops the keys.
    if (wantTimeBounds) {
      try {
        final List<Row> rows = run(timeBoundsQuery(dataSource, intervals, filter));
        if (!rows.isEmpty()) {
          final Long min = asLong(rows.get(0).getRaw("minTime"));
          final Long max = asLong(rows.get(0).getRaw("maxTime"));
          if (min != null && max != null) {
            out.put("timeBounds", Lists.newArrayList(iso(min), iso(max)));
          }
        }
      }
      catch (Exception e) {
        out.put("timeBoundsError", String.valueOf(e.getMessage()));
      }
    }
    return Response.ok(out).build();
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

  private Map<String, Object> timeBoundsQuery(String ds, List<String> intervals, Object filter)
  {
    final Map<String, Object> q = Maps.newLinkedHashMap();
    q.put("queryType", "timeseries");
    q.put("dataSource", ds);
    q.put("intervals", intervals);
    q.put("granularity", "all");
    if (filter != null) {
      q.put("filter", filter);
    }
    q.put("aggregations", Lists.newArrayList(
        ImmutableMap.of("type", "longMin", "name", "minTime", "fieldName", "__time"),
        ImmutableMap.of("type", "longMax", "name", "maxTime", "fieldName", "__time")
    ));
    // finalize=false: longMin/longMax finalize to an OptionalLong which the direct-historical merge chokes on
    // ("OptionalLong cannot be cast to Number"); the unfinalized value is a plain long we read directly.
    q.put("context", ImmutableMap.of("timeout", TIMEOUT_MS, "queryId", newId(), "finalize", false));
    return q;
  }

  @SuppressWarnings("unchecked")
  private <T> List<T> run(Map<String, Object> queryMap)
  {
    final Query<T> query = (Query<T>) mapper.convertValue(queryMap, Query.class);
    // Register with the QueryManager so it gets a SessionCache (queries run outside the normal QueryResource path
    // otherwise NPE on QueryWatcher.getSessionCache) + a cancellation handle; unregister when done.
    queryManager.register(query, SettableFuture.create(), null);
    try {
      final Sequence<T> sequence = query.run(walker, Maps.newHashMap());
      return Sequences.toList(sequence);
    }
    finally {
      queryManager.unregister(query, null);
    }
  }

  private static String newId()
  {
    return "resolve-" + java.util.UUID.randomUUID();
  }

  // longMin/longMax return an OptionalLong (empty when no matching rows); also tolerate a plain Number.
  private static Long asLong(Object v)
  {
    if (v instanceof java.util.OptionalLong) {
      final java.util.OptionalLong o = (java.util.OptionalLong) v;
      return o.isPresent() ? o.getAsLong() : null;
    }
    return v instanceof Number ? ((Number) v).longValue() : null;
  }

  private static String iso(long millis)
  {
    return new DateTime(millis, DateTimeZone.UTC).toString();
  }

  private static String intervalOf(Object interval)
  {
    if (interval instanceof List && ((List<?>) interval).size() == 2) {
      final List<?> iv = (List<?>) interval;
      return iv.get(0) + "/" + iv.get(1);
    }
    return interval instanceof String ? (String) interval : ETERNITY;
  }
}
