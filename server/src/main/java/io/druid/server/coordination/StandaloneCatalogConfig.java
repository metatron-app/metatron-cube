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

package io.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;

/**
 * {@code druid.standalone.sourceTables} — a static dataSource -&gt; source-table map the {@code /catalog} endpoint
 * reports so the Trino connector knows which source table each Druid dataSource was ingested from. Existing segments
 * carry no source-table provenance in their metadata, so this is supplied by config (JSON map in runtime.properties).
 */
public class StandaloneCatalogConfig
{
  @JsonProperty
  private Map<String, String> sourceTables = Collections.emptyMap();

  // dataSource -> the SOURCE table's timestamp column that became Druid's __time (e.g. atom_credential ->
  // timestamp_trigger). Dimensions/metrics keep their source names, so only __time needs a mapping; the /schema
  // endpoint reports it as the __time column's sourceColumn.
  @JsonProperty
  private Map<String, String> timeColumns = Collections.emptyMap();

  public Map<String, String> getSourceTables()
  {
    return sourceTables;
  }

  public Map<String, String> getTimeColumns()
  {
    return timeColumns;
  }
}
