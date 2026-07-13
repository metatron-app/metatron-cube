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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.server.coordination.ServerManager;
import io.druid.server.coordination.StandaloneCatalogConfig;
import io.druid.server.security.AuthConfig;
import org.joda.time.Interval;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The served catalog for the Trino connector: for every loaded dataSource, the source table it was ingested from
 * (from {@code druid.standalone.sourceTables} config) and the indexed time span (min..max of its loaded segments).
 * Lets the connector route a query on a source table to Druid only for the interval Druid actually covers.
 *
 * <pre>GET /druid/v2/catalog
 * [{"dataSource":"atom_credential","sourceTable":"iceberg.atom....v2","interval":["2026-04-20T..Z","2026-07-09T..Z"]}]</pre>
 */
@Path("/druid/v2/catalog")
public class CatalogResource
{
  private final ServerManager serverManager;
  private final StandaloneCatalogConfig config;

  @Inject
  public CatalogResource(ServerManager serverManager, StandaloneCatalogConfig config)
  {
    this.serverManager = serverManager;
    this.config = config;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCatalog(@Context HttpServletRequest req)
  {
    // internal, network-gated endpoint with no per-datasource authz — mark checked so the response filter
    // (PreResponseAuthorizationCheckFilter) doesn't WARN on every call.
    if (req != null) {
      req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    }
    final Map<String, String> sourceTables = config.getSourceTables();
    final List<Map<String, Object>> entries = Lists.newArrayList();
    for (String dataSource : serverManager.getLoadedDataSources()) {
      final Map<String, Object> entry = Maps.newLinkedHashMap();
      entry.put("dataSource", dataSource);
      entry.put("sourceTable", sourceTables.get(dataSource));   // null if not configured
      final Interval interval = serverManager.getIndexedInterval(dataSource);
      if (interval != null) {
        entry.put("interval", Arrays.asList(interval.getStart().toString(), interval.getEnd().toString()));
      }
      entries.add(entry);
    }
    return Response.ok(entries).build();
  }
}
