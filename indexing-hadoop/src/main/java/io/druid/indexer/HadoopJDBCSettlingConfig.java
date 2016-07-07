/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.aggregation.AggregatorFactory;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.List;
import java.util.Map;

public class HadoopJDBCSettlingConfig extends HadoopSettlingConfig
{
  final MetadataStorageConnectorConfig config;
  final String query;

  @JsonCreator
  public HadoopJDBCSettlingConfig(
      @JsonProperty(value = "connectorConfig", required = true) final MetadataStorageConnectorConfig connectorConfig,
      @JsonProperty(value = "query", required = true) final String query,
      @JsonProperty(value = "constColumns", required = true) final List<String> constColumns,
      @JsonProperty(value = "regexColumns", required = true) final List<String> regexColumns,
      @JsonProperty(value = "paramNameColumn", required = true) final String paramNameColumn,
      @JsonProperty(value = "paramValueColumn", required = true) final String paramValueColumn,
      @JsonProperty(value = "typeColumn", required = true) final String aggTypeColumn,
      @JsonProperty(value = "offsetColumn", required = true) final String offset,
      @JsonProperty(value = "sizeColumn", required = true) final String size,
      @JsonProperty(value = "settlingYNColumn") final String settlingYN
  )
  {
    super(constColumns, regexColumns, paramNameColumn, paramValueColumn, aggTypeColumn, offset, size, settlingYN);

    this.config = connectorConfig;
    this.query = query;
  }

  @JsonProperty("connectorConfig")
  public MetadataStorageConnectorConfig getConfig()
  {
    return config;
  }

  @JsonProperty("query")
  public String getQuery()
  {
    return query;
  }

  @Override
  public Settler setUp(AggregatorFactory[] org)
  {
    // connect through the given connector
    final Handle handle = new DBI(
        config.getConnectURI(),
        config.getUser(),
        config.getPassword()
    ).open();

    // fill the Map
    List<Map<String, Object>> maps = handle.select(query);
    handle.close();

    return super.setUp(org, maps);
  }
}
