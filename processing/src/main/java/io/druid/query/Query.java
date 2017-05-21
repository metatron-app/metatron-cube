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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import io.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.PartitionedGroupByQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.select.SelectForwardQuery;
import io.druid.query.select.SelectMetaQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.VirtualColumn;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "queryType")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = Query.TIMESERIES, value = TimeseriesQuery.class),
    @JsonSubTypes.Type(name = Query.SEARCH, value = SearchQuery.class),
    @JsonSubTypes.Type(name = Query.TIME_BOUNDARY, value = TimeBoundaryQuery.class),
    @JsonSubTypes.Type(name = Query.GROUP_BY, value = GroupByQuery.class),
    @JsonSubTypes.Type(name = Query.SEGMENT_METADATA, value = SegmentMetadataQuery.class),
    @JsonSubTypes.Type(name = Query.SELECT, value = SelectQuery.class),
    @JsonSubTypes.Type(name = Query.SELECT_META, value = SelectMetaQuery.class),
    @JsonSubTypes.Type(name = Query.SELECT_STREAM, value = StreamQuery.class),
    @JsonSubTypes.Type(name = Query.TOPN, value = TopNQuery.class),
    @JsonSubTypes.Type(name = Query.DATASOURCE_METADATA, value = DataSourceMetadataQuery.class),
    @JsonSubTypes.Type(name = Query.UNION_ALL, value = UnionAllQuery.class),
    @JsonSubTypes.Type(name = Query.JOIN, value = JoinQuery.class),
    @JsonSubTypes.Type(name = Query.GROUP_BY_PARTITIONED, value = PartitionedGroupByQuery.class),
    @JsonSubTypes.Type(name = Query.SELECT_DELEGATE, value = SelectForwardQuery.class),
})
public interface Query<T> extends QueryContextKeys
{
  String TIMESERIES = "timeseries";
  String SEARCH = "search";
  String TIME_BOUNDARY = "timeBoundary";
  String GROUP_BY = "groupBy";
  String GROUP_BY_PARTITIONED = "groupBy.partitioned";
  String SEGMENT_METADATA = "segmentMetadata";
  String SELECT = "select";
  String SELECT_META = "selectMeta";
  String SELECT_STREAM = "select.stream";
  String SELECT_DELEGATE = "select.delegate";
  String TOPN = "topN";
  String DATASOURCE_METADATA = "dataSourceMetadata";
  String UNION_ALL = "unionAll";
  String JOIN = "join";

  DataSource getDataSource();

  boolean hasFilters();

  String getType();

  Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context);

  Sequence<T> run(QueryRunner<T> runner, Map<String, Object> context);

  List<Interval> getIntervals();

  QuerySegmentSpec getQuerySegmentSpec();

  Duration getDuration();

  Map<String, Object> getContext();

  <ContextType> ContextType getContextValue(String key);

  <ContextType> ContextType getContextValue(String key, ContextType defaultValue);

  boolean getContextBoolean(String key, boolean defaultValue);

  int getContextInt(String key, int defaultValue);

  boolean isDescending();

  Ordering<T> getResultOrdering();

  Query<T> withOverriddenContext(Map<String, Object> contextOverride);

  Query<T> withQuerySegmentSpec(QuerySegmentSpec spec);

  Query<T> withId(String id);

  String getId();

  Query<T> withDataSource(DataSource dataSource);

  interface DimFilterSupport<T> extends Query<T>
  {
    DimFilter getDimFilter();

    DimFilterSupport<T> withDimFilter(DimFilter filter);
  }

  interface DimensionSupport<T> extends DimFilterSupport<T>
  {
    List<DimensionSpec> getDimensions();

    List<VirtualColumn> getVirtualColumns();

    DimensionSupport<T> withDimensionSpecs(List<DimensionSpec> dimensions);

    DimensionSupport<T> withVirtualColumns(List<VirtualColumn> virtualColumns);

    boolean allDimensionsForEmpty();
  }

  interface ViewSupport<T> extends DimFilterSupport<T>, DimensionSupport<T>
  {
    List<String> getMetrics();

    ViewSupport<T> withMetrics(List<String> metrics);

    boolean allMetricsForEmpty();
  }

  interface RewritingQuery<T> extends Query<T>
  {
    Query rewriteQuery(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper);
  }
}
