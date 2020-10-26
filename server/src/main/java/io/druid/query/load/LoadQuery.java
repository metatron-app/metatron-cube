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

package io.druid.query.load;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.server.FileLoadSpec;

import java.util.Map;
import java.util.Set;

import static io.druid.server.ServiceTypes.BROKER;

@JsonTypeName("load")
public class LoadQuery extends BaseQuery<Map<String, Object>> implements Query.ManagementQuery
{
  public static LoadQuery of(FileLoadSpec loadSpec)
  {
    return new LoadQuery(null, null, loadSpec, null);
  }

  private final FileLoadSpec loadSpec;

  public LoadQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("loadSpec") FileLoadSpec loadSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource == null ? TableDataSource.of("load") : dataSource,
        querySegmentSpec,
        false,
        context
    );
    this.loadSpec = Preconditions.checkNotNull(loadSpec, "'loadSpec' is missing");
  }

  @JsonProperty
  public FileLoadSpec getLoadSpec()
  {
    return loadSpec;
  }

  @Override
  public String getType()
  {
    return "load";
  }

  @Override
  public Query<Map<String, Object>> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new LoadQuery(getDataSource(), getQuerySegmentSpec(), loadSpec, computeOverriddenContext(contextOverride));
  }

  @Override
  public Query<Map<String, Object>> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new LoadQuery(getDataSource(), spec, loadSpec, getContext());
  }

  @Override
  public Query<Map<String, Object>> withDataSource(DataSource dataSource)
  {
    return new LoadQuery(dataSource, getQuerySegmentSpec(), loadSpec, getContext());
  }

  @Override
  public Set<String> supports()
  {
    return ImmutableSet.of(BROKER);
  }

  @Override
  public String toString()
  {
    return "LoadQuery{loadSpec=" + loadSpec + '}';
  }
}
