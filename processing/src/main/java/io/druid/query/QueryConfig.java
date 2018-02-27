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

package io.druid.query;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryConfig;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamRawQuery;
import io.druid.query.topn.TopNQueryConfig;

import javax.validation.constraints.NotNull;

/**
 */
public class QueryConfig
{
  @JsonProperty
  public int maxResults;

  @JsonProperty
  public boolean useCustomSerdeForDateTime;

  @JacksonInject
  @NotNull
  public GroupByQueryConfig groupBy = new GroupByQueryConfig();

  @JacksonInject
  @NotNull
  public SearchQueryConfig search = new SearchQueryConfig();

  @JacksonInject
  @NotNull
  public SelectQueryConfig select = new SelectQueryConfig();

  @JacksonInject
  @NotNull
  public TopNQueryConfig topN = new TopNQueryConfig();

  @JacksonInject
  @NotNull
  public SegmentMetadataQueryConfig segmentMeta = new SegmentMetadataQueryConfig();

  @JacksonInject
  @NotNull
  public JoinQueryConfig join = new JoinQueryConfig();

  public int getMaxResults()
  {
    return maxResults <= 0 ? groupBy.getMaxResults() : maxResults;
  }

  public boolean useCustomSerdeForDateTime(Query query)
  {
    if (!useCustomSerdeForDateTime || query.getContextBoolean(Query.DATETIME_STRING_SERDE, false) ) {
      return false;
    }
    // events containing DateTime in Map
    if (query instanceof SelectQuery || query instanceof StreamQuery || query instanceof StreamRawQuery) {
      return !select.isUseDateTime();
    }
    return true;
  }
}
