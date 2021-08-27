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
import io.druid.query.frequency.FrequencyQueryConfig;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryConfig;
import io.druid.query.select.StreamQuery;
import io.druid.query.topn.TopNQueryConfig;

import javax.validation.constraints.Min;
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
  public boolean useHistoricalNodesOnlyForLuceneFilter;

  @JsonProperty
  @Min(1_000)
  private long maxQueryTimeout = 60_000;

  @JsonProperty
  private boolean useCuboids;

  @JsonProperty
  @Min(1)
  private int maxQueryParallelism = 8;

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

  @JacksonInject
  @NotNull
  public Supplier<FrequencyQueryConfig> frequency = Suppliers.ofInstance(new FrequencyQueryConfig());

  public int getMaxResults(Query<?> query)
  {
    return maxedBySystem(query, Query.MAX_RESULTS, maxResults <= 0 ? getGroupBy().getMaxResults() : maxResults);
  }

  public int getQueryParallelism(Query<?> query)
  {
    return maxedBySystem(query, Query.MAX_QUERY_PARALLELISM, maxQueryParallelism);
  }

  public int getHashJoinThreshold(Query<?> query)
  {
    return maxedBySystem(query, Query.HASHJOIN_THRESHOLD, getJoin().getHashJoinThreshold());
  }

  public int getSemiJoinThreshold(Query<?> query)
  {
    return maxedBySystem(query, Query.SEMIJOIN_THRESHOLD, getJoin().getSemiJoinThreshold());
  }

  public int getBroadcastJoinThreshold(Query<?> query)
  {
    return maxedBySystem(query, Query.BROADCASTJOIN_THRESHOLD, getJoin().getBroadcastJoinThreshold());
  }

  public int getBloomFilterThreshold(Query<?> query)
  {
    return maxedBySystem(query, Query.BLOOMFILTER_THRESHOLD, getJoin().getBloomFilterThreshold());
  }

  public int getForcedFilterHugeThreshold(Query<?> query)
  {
    return maxedBySystem(query, Query.FORCEDFILTER_HUGE_THRESHOLD, getJoin().getForcedFilterHugeThreshold());
  }

  public int getForcedFilterTinyThreshold(Query<?> query)
  {
    return maxedBySystem(query, Query.FORCEDFILTER_TINY_THRESHOLD, getJoin().getForcedFilterTinyThreshold());
  }

  public int getMaxMultiValueDimensions(Query<?> query)
  {
    return maxedBySystem(query, Query.GBY_MAX_MULTIVALUE_DIMENSIONS, getGroupBy().getMaxMultiValueDimensions());
  }

  public boolean isGroupedUnfoldDimensions(Query<?> query)
  {
    return query.getContextBoolean(Query.GBY_GROUPED_UNFOLD_DIMENSIONS, getGroupBy().isGroupedUnfoldDimensions());
  }

  private int maxedBySystem(Query<?> query, String key, int systemMax)
  {
    if (!query.hasContext(key)) {
      return systemMax;
    }
    int userMax = query.getContextInt(key, -1);
    return userMax <= 0 ? systemMax : systemMax <= 0 ? userMax : Math.min(systemMax, userMax);
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

  public boolean isUseHistoricalNodesOnlyForLuceneFilter()
  {
    return useHistoricalNodesOnlyForLuceneFilter;
  }

  public boolean useBulkRow(Query<?> query)
  {
    if (BaseQuery.isBySegment(query)) {
      return false;
    }
    if (query instanceof GroupByQuery &&
        query.getContextBoolean(Query.GBY_USE_BULK_ROW, groupBy.get().isUseBulkRow())) {
      return true;
    }
    if (query instanceof StreamQuery &&
        query.getContextBoolean(Query.STREAM_USE_BULK_ROW, select.get().isUseBulkRow())) {
      return true;
    }
    return false;
  }

  public boolean isUseCuboids()
  {
    return useCuboids;
  }

  public void setUseCuboids(boolean useCuboids)
  {
    this.useCuboids = useCuboids;
  }

  public long getMaxQueryTimeout()
  {
    return maxQueryTimeout;
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

  public FrequencyQueryConfig getFrequency()
  {
    return frequency.get();
  }
}
