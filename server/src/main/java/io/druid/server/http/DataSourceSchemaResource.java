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

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
  private final ServerManager serverManager;

  @Inject
  public DataSourceSchemaResource(ServerManager serverManager)
  {
    this.serverManager = serverManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSchema(@PathParam("dataSourceName") String dataSourceName)
  {
    final QueryableIndex index = serverManager.getRepresentativeIndex(dataSourceName);
    if (index == null) {
      return Response.status(Response.Status.NOT_FOUND)
                     .entity(ImmutableMap.of("error", "no segments loaded for dataSource[" + dataSourceName + "]"))
                     .build();
    }
    final Set<String> dimensions = Sets.newHashSet(index.getAvailableDimensions());
    final List<Map<String, Object>> columns = Lists.newArrayList();
    columns.add(column(Column.TIME_COLUMN_NAME, "LONG", false, false));
    for (String name : index.getColumnNames()) {
      if (Column.TIME_COLUMN_NAME.equals(name)) {
        continue;
      }
      final Column column = index.getColumn(name);
      if (column == null) {
        continue;
      }
      final ColumnCapabilities caps = column.getCapabilities();
      columns.add(column(name, typeOf(caps, column), caps != null && caps.hasLuceneIndex(), dimensions.contains(name)));
    }
    return Response.ok(ImmutableMap.of("dataSource", dataSourceName, "columns", columns)).build();
  }

  private static String typeOf(ColumnCapabilities caps, Column column)
  {
    if (caps != null && caps.getType() != null) {
      return caps.getType().name();
    }
    final ValueDesc type = column.getType();
    return type == null ? "STRING" : type.typeName().toUpperCase();
  }

  private static Map<String, Object> column(String name, String type, boolean luceneIndexed, boolean prunable)
  {
    final Map<String, Object> m = Maps.newLinkedHashMap();
    m.put("name", name);
    m.put("type", type);
    if (luceneIndexed) {
      m.put("luceneIndexed", true);   // connector can push a lucene filter to this column
    }
    if (prunable) {
      m.put("prunable", true);        // dictionary-encoded dimension: =/IN predicates prune segments
    }
    return m;
  }
}
