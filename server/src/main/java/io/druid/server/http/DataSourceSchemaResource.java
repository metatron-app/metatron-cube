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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.data.ValueDesc;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.server.coordination.ServerManager;
import io.druid.server.coordination.StandaloneCatalogConfig;
import io.druid.server.security.AuthConfig;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema of a served datasource for the Trino connector: the columns, their types, and which columns carry a
 * secondary index the connector can push predicates into. Two flags:
 * <ul>
 *   <li>{@code luceneIndexed} — the column has a lucene index ({@code raw}); the connector can push a lucene filter
 *       down to it.</li>
 *   <li>{@code prunable} — the column is a dictionary-encoded dimension; an equality/IN predicate on it lets the
 *       historical prune segments by scanning the (small) dictionary before fetching the column.</li>
 * </ul>
 * Flags are emitted only when true (a plain column carries neither). The capabilities come from the segment header,
 * so this is cheap (one lucene head fetch at most) and safe to call at planning time.
 */
@Path("/druid/v2/datasources/{dataSourceName}/schema")
public class DataSourceSchemaResource
{
  private static final String SCORE_COLUMN = "_score";

  private final ServerManager serverManager;
  private final StandaloneCatalogConfig config;

  @Inject
  public DataSourceSchemaResource(ServerManager serverManager, StandaloneCatalogConfig config)
  {
    this.serverManager = serverManager;
    this.config = config;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSchema(@PathParam("dataSourceName") String dataSourceName, @Context HttpServletRequest req)
  {
    // internal, network-gated endpoint with no per-datasource authz — mark checked so the response filter
    // (PreResponseAuthorizationCheckFilter) doesn't WARN on every call.
    if (req != null) {
      req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    }
    final QueryableIndex index = serverManager.getRepresentativeIndex(dataSourceName);
    if (index == null) {
      return Response.status(Response.Status.NOT_FOUND)
                     .entity(ImmutableMap.of("error", "no segments loaded for dataSource[" + dataSourceName + "]"))
                     .build();
    }
    final Set<String> dimensions = Sets.newHashSet(index.getAvailableDimensions());
    final List<Map<String, Object>> columns = Lists.newArrayList();
    columns.add(timeColumn(config.getTimeColumns().get(dataSourceName)));
    boolean anyLucene = false;
    for (String name : index.getColumnNames()) {
      if (Column.TIME_COLUMN_NAME.equals(name)) {
        continue;
      }
      final Column column = index.getColumn(name);
      if (column == null) {
        continue;
      }
      final ColumnCapabilities caps = column.getCapabilities();
      final boolean lucene = caps != null && caps.hasLuceneIndex();
      anyLucene |= lucene;
      columns.add(column(name, typeOf(caps, column), lucene, dimensions.contains(name)));
    }
    if (anyLucene) {
      // relevance score is available only when a lucene filter is present; expose it as a projectable column.
      columns.add(scoreColumn());
    }
    final Map<String, Object> out = Maps.newLinkedHashMap();
    out.put("dataSource", dataSourceName);
    out.put("queryTemplate", queryTemplate(dataSourceName));
    out.put("columns", columns);
    if (anyLucene) {
      out.put("scoring", scoring());
    }
    return Response.ok(out).build();
  }

  /**
   * The select.stream skeleton the connector fills. {@code ${intervals}} (array), {@code ${filter}} (the AND of the
   * column pushdown fragments, or omitted), {@code ${columns}} (projection array) and {@code ${limit}} (int) are
   * structural placeholders the connector substitutes. Kept server-side so the query shape can evolve without a
   * connector rebuild.
   */
  private static Map<String, Object> queryTemplate(String dataSource)
  {
    final Map<String, Object> t = Maps.newLinkedHashMap();
    t.put("queryType", "select.stream");
    t.put("dataSource", dataSource);
    t.put("intervals", "${intervals}");
    t.put("filter", "${filter}");
    t.put("columns", "${columns}");
    // ${virtualColumns} carries the relevance-score attachment column (see the `scoring` block) when _score is
    // projected; the connector substitutes [] otherwise.
    t.put("virtualColumns", "${virtualColumns}");
    t.put("limitSpec", ImmutableMap.of("type", "default", "limit", "${limit}"));
    return t;
  }

  /**
   * The relevance-score column, present only when the datasource has a lucene-indexed column. It is not a stored
   * column — its per-row value is produced by the lucene filter (see {@link #scoring()}), so it exists only in a
   * query that both carries a lucene filter and opts into scoring.
   */
  private static Map<String, Object> scoreColumn()
  {
    final Map<String, Object> m = Maps.newLinkedHashMap();
    m.put("name", SCORE_COLUMN);
    m.put("type", "DOUBLE");
    m.put("relevanceScore", true);   // synthetic: only materialized when scoring is enabled on the query
    return m;
  }

  /**
   * How the connector turns a projected/ordered {@code _score} into a scored query. Scoring is NOT pushed as an
   * ORDER BY — the ordered select.stream path skips the per-segment scoring pass, so {@code _score} would come back
   * null. Instead the connector, whenever the query references {@code _score}:
   * <ol>
   *   <li>adds {@code "scoreField": "_score"} (and optionally {@code "limit": N} = per-segment top-N by score) to the
   *       {@code lucene.query} filter fragment it already emits for the {@code match} pushdown;</li>
   *   <li>adds {@code virtualColumn} to the query's {@code virtualColumns} ({@code ${virtualColumns}} in the
   *       template) and {@code _score} to {@code columns};</li>
   *   <li>performs any {@code ORDER BY _score} itself (Trino-side) — {@code pushOrdering} is false — since the score
   *       is materialized per row but not sortable inside the stream.</li>
   * </ol>
   */
  private static Map<String, Object> scoring()
  {
    final Map<String, Object> s = Maps.newLinkedHashMap();
    s.put("column", SCORE_COLUMN);
    s.put("type", "DOUBLE");
    s.put("from", "lucene.query");     // scores are produced by this filter type; it must be present in the query
    s.put("scoreField", SCORE_COLUMN); // add this key to the lucene.query filter fragment to emit the score
    s.put("limitField", "limit");      // optional cap on that same fragment: per-segment top-N docs by score
    s.put("virtualColumn", ImmutableMap.of(
        "type", "$attachment", "outputName", SCORE_COLUMN, "columnType", "FLOAT"
    ));
    s.put("pushOrdering", false);      // ORDER BY _score must be done by the connector/Trino, not pushed to Druid
    return s;
  }

  private static String typeOf(ColumnCapabilities caps, Column column)
  {
    if (caps != null && caps.getType() != null) {
      return caps.getType().name();
    }
    final ValueDesc type = column.getType();
    return type == null ? "STRING" : type.typeName().toUpperCase();
  }

  private static Map<String, Object> timeColumn(String sourceColumn)
  {
    final Map<String, Object> m = Maps.newLinkedHashMap();
    m.put("name", Column.TIME_COLUMN_NAME);
    m.put("type", "LONG");
    if (sourceColumn != null) {
      // the source table's timestamp column that became __time (dimensions/metrics keep their source names). A
      // range predicate on this source column maps to __time -> the query intervals.
      m.put("sourceColumn", sourceColumn);
    }
    // a time range predicate is pushed to the query intervals, not to a filter
    m.put("pushdown", ImmutableMap.of("range", ImmutableMap.of("target", "intervals")));
    return m;
  }

  /**
   * One column's schema + its pushdown templates: for each Trino predicate kind the connector can push, the Druid
   * filter fragment to emit (with {@code ${value}} / {@code ${values}} placeholders). The connector is a generic
   * filler — it needn't know a column is lucene-indexed, only that a {@code match} predicate maps to this fragment —
   * so a new server-side capability flows through by adding a template entry, no connector change.
   */
  private static Map<String, Object> column(String name, String type, boolean luceneIndexed, boolean prunable)
  {
    final Map<String, Object> m = Maps.newLinkedHashMap();
    m.put("name", name);
    m.put("type", type);
    final Map<String, Object> pushdown = Maps.newLinkedHashMap();
    if (luceneIndexed) {
      m.put("luceneIndexed", true);   // human-readable hint
      // full-text / contains -> lucene filter
      pushdown.put("match", ImmutableMap.of("type", "lucene.query", "field", name, "expression", "${value}"));
    }
    if (prunable) {
      m.put("prunable", true);        // dictionary-encoded dimension: =/IN prune segments via dict-scan
      pushdown.put("equals", ImmutableMap.of("type", "selector", "dimension", name, "value", "${value}"));
      pushdown.put("in", ImmutableMap.of("type", "in", "dimension", name, "values", "${values}"));
    }
    if (!pushdown.isEmpty()) {
      m.put("pushdown", pushdown);
    }
    return m;
  }
}
