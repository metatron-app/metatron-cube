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

package io.druid.query;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryConfig;
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

  @JsonProperty
  public boolean useHandedOffSegmentsOnlyForLuceneIndex;

  @JacksonInject
  @NotNull
  public Supplier<GroupByQueryConfig> groupBy = Suppliers.ofInstance(new GroupByQueryConfig());

  @JacksonInject
  @NotNull
  public Supplier<SearchQueryConfig> search = Suppliers.ofInstance(new SearchQueryConfig());

  @JacksonInject
  @NotNull
  public Supplier<SelectQueryConfig> select = Suppliers.ofInstance(new SelectQueryConfig());

  @JacksonInject
  @NotNull
  public Supplier<TopNQueryConfig> topN = Suppliers.ofInstance(new TopNQueryConfig());

  @JacksonInject
  @NotNull
  public Supplier<SegmentMetadataQueryConfig> segmentMeta = Suppliers.ofInstance(new SegmentMetadataQueryConfig());

  @JacksonInject
  @NotNull
  public Supplier<JoinQueryConfig> join = Suppliers.ofInstance(new JoinQueryConfig());

  public int getMaxResults(Query<?> query)
  {
    int systemMax = maxResults <= 0 ? getGroupBy().getMaxResults() : maxResults;
    int userMax = query.getContextValue(Query.MAX_RESULTS, -1);
    return userMax <= 0 ? systemMax : Math.min(systemMax, userMax);
  }

  public int getMaxMergeParallelism(Query<?> query)
  {
    final int systemMax = getGroupBy().getMaxMergeParallelism();
    final int userMax = query.getContextInt(Query.GBY_MERGE_PARALLELISM, -1);
    return userMax <= 0 ? systemMax : Math.min(systemMax, userMax);
  }

  public int getHashJoinThreshold(Query<?> query)
  {
    final int systemMax = getJoin().getHashJoinThreshold();
    final int userMax = query.getContextInt(Query.HASHJOIN_THRESHOLD, -1);
    return userMax <= 0 ? systemMax : Math.min(systemMax, userMax);
  }

  public boolean useParallelSort(Query<?> query)
  {
    return query.getContextBoolean(Query.GBY_USE_PARALLEL_SORT, getGroupBy().isUseParallelSort());
  }

  public boolean useCustomSerdeForDateTime(Query query)
  {
    if (!useCustomSerdeForDateTime || query.getContextBoolean(Query.DATETIME_STRING_SERDE, false) ) {
      return false;
    }
    if (query instanceof SelectQuery) {
      // events containing DateTime in Map (which means no type information)
      return !getSelect().isUseDateTime();
    }
    return true;
  }

  public boolean isUseHandedOffSegmentsOnlyForLuceneIndex()
  {
    return useHandedOffSegmentsOnlyForLuceneIndex;
  }

  public boolean useBulkRow(Query query)
  {
    return !BaseQuery.isBySegment(query) &&
           query instanceof GroupByQuery &&
           query.getContextBoolean(Query.GBY_USE_BULK_ROW, groupBy.get().isUseBulkRow());
  }

  public GroupByQueryConfig getGroupBy()
  {
    return groupBy.get();
  }

  public SearchQueryConfig getSearch()
  {
    return search.get();
  }

  public SelectQueryConfig getSelect()
  {
    return select.get();
  }

  public TopNQueryConfig getTopN()
  {
    return topN.get();
  }

  public SegmentMetadataQueryConfig getSegmentMeta()
  {
    return segmentMeta.get();
  }

  public JoinQueryConfig getJoin()
  {
    return join.get();
  }
}
