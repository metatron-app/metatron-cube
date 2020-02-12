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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Supplier;
import com.google.common.collect.Ordering;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByMetaQuery;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.kmeans.FindNearestQuery;
import io.druid.query.kmeans.KMeansQuery;
import io.druid.query.kmeans.KMeansTaggingQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.select.SchemaQuery;
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

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "queryType")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = Query.TIMESERIES, value = TimeseriesQuery.class),
    @JsonSubTypes.Type(name = Query.SEARCH, value = SearchQuery.class),
    @JsonSubTypes.Type(name = Query.TIME_BOUNDARY, value = TimeBoundaryQuery.class),
    @JsonSubTypes.Type(name = Query.GROUP_BY, value = GroupByQuery.class),
    @JsonSubTypes.Type(name = Query.GROUP_BY_META, value = GroupByMetaQuery.class),
    @JsonSubTypes.Type(name = Query.SEGMENT_METADATA, value = SegmentMetadataQuery.class),
    @JsonSubTypes.Type(name = Query.SELECT, value = SelectQuery.class),
    @JsonSubTypes.Type(name = Query.SELECT_META, value = SelectMetaQuery.class),
    @JsonSubTypes.Type(name = Query.SCHEMA, value = SchemaQuery.class),
    @JsonSubTypes.Type(name = Query.SELECT_STREAM, value = StreamQuery.class),
    @JsonSubTypes.Type(name = Query.TOPN, value = TopNQuery.class),
    @JsonSubTypes.Type(name = Query.DATASOURCE_METADATA, value = DataSourceMetadataQuery.class),
    @JsonSubTypes.Type(name = Query.UNION_ALL, value = UnionAllQuery.class),
    @JsonSubTypes.Type(name = Query.JOIN, value = JoinQuery.class),
    @JsonSubTypes.Type(name = Query.SELECT_DELEGATE, value = SelectForwardQuery.class),
    @JsonSubTypes.Type(name = "kmeans", value = KMeansQuery.class),
    @JsonSubTypes.Type(name = "kmeans.nearest", value = FindNearestQuery.class),
    @JsonSubTypes.Type(name = "kmeans.tagging", value = KMeansTaggingQuery.class),
    @JsonSubTypes.Type(name = "classify", value = ClassifyQuery.class),
})
public interface Query<T> extends QueryContextKeys
{
  String TIMESERIES = "timeseries";
  String SEARCH = "search";
  String TIME_BOUNDARY = "timeBoundary";
  String GROUP_BY = "groupBy";
  String GROUP_BY_META = "groupBy.meta";
  String SEGMENT_METADATA = "segmentMetadata";
  String SELECT = "select";
  String SELECT_META = "selectMeta";
  String SCHEMA = "schema";
  String SELECT_STREAM = "select.stream";
  String SELECT_DELEGATE = "select.delegate";
  String TOPN = "topN";
  String DATASOURCE_METADATA = "dataSourceMetadata";
  String UNION_ALL = "unionAll";
  String ITERATE = "iterate";
  String JOIN = "join";

  DataSource getDataSource();

  boolean hasFilters();

  Granularity getGranularity();

  String getType();

  Query<T> resolveQuery(Supplier<RowResolver> resolver);

  Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context);

  List<Interval> getIntervals();

  QuerySegmentSpec getQuerySegmentSpec();

  Duration getDuration();

  Map<String, Object> getContext();

  <ContextType> ContextType getContextValue(String key);

  <ContextType> ContextType getContextValue(String key, ContextType defaultValue);

  boolean getContextBoolean(String key, boolean defaultValue);

  int getContextInt(String key, int defaultValue);

  long getContextLong(String key, long defaultValue);

  boolean isDescending();

  // used for merging partial results.. return null if no need to (concat all: see stream query)
  Ordering<T> getMergeOrdering();

  Query<T> withOverriddenContext(Map<String, Object> contextOverride);

  Query<T> withOverriddenContext(String contextKey, Object contextValue);

  Query<T> withQuerySegmentSpec(QuerySegmentSpec spec);

  Query<T> toLocalQuery();

  Query<T> withId(String id);

  String getId();

  Query<T> withSqlQueryId(String sqlQueryId);

  @Nullable
  String getSqlQueryId();

  Query<T> withDataSource(DataSource dataSource);

  interface VCSupport<T> extends Query<T>
  {
    List<VirtualColumn> getVirtualColumns();

    VCSupport<T> withVirtualColumns(List<VirtualColumn> virtualColumns);
  }

  interface FilterSupport<T> extends VCSupport<T>
  {
    DimFilter getFilter();

    FilterSupport<T> withFilter(DimFilter filter);
  }

  interface ColumnsSupport<T> extends FilterSupport<T>
  {
    List<String> getColumns();

    ColumnsSupport<T> withColumns(List<String> columns);
  }

  interface DimensionSupport<T> extends FilterSupport<T>
  {
    List<DimensionSpec> getDimensions();

    DimensionSupport<T> withDimensionSpecs(List<DimensionSpec> dimensions);

    boolean allDimensionsForEmpty();
  }

  interface MetricSupport<T> extends DimensionSupport<T>
  {
    List<String> getMetrics();

    MetricSupport<T> withMetrics(List<String> metrics);

    boolean allMetricsForEmpty();
  }

  interface AggregationsSupport<T> extends DimensionSupport<T>, ArrayOutputSupport<T>
  {
    List<AggregatorFactory> getAggregatorSpecs();

    AggregationsSupport<T> withAggregatorSpecs(List<AggregatorFactory> metrics);

    List<PostAggregator> getPostAggregatorSpecs();

    AggregationsSupport<T> withPostAggregatorSpecs(List<PostAggregator> metrics);

    boolean allMetricsForEmpty();
  }

  Logger LOG = new Logger(ArrayOutputSupport.class);
  interface ArrayOutputSupport<T> extends Query<T>
  {
    List<String> estimatedOutputColumns();

    Sequence<Object[]> array(Sequence<T> sequence);
  }

  interface OrderingSupport<T> extends Query<T>
  {
    List<OrderByColumnSpec> getResultOrdering();

    OrderingSupport<T> withResultOrdering(List<OrderByColumnSpec> orderingSpecs);
  }

  interface LateralViewSupport<T> extends Query<T>
  {
    LateralViewSpec getLateralView();

    LateralViewSupport<T> withLateralView(LateralViewSpec lateralViewSpec);
  }

  interface RewritingQuery<T> extends Query<T>
  {
    Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig);
  }

  interface IteratingQuery<INTERMEDIATE, FINAL>
  {
    Pair<Sequence<FINAL>, Query<INTERMEDIATE>> next(Sequence<INTERMEDIATE> sequence, Query<INTERMEDIATE> prev);
  }

  // marker.. broadcasts query to all known servers (see CCC)
  interface ManagementQuery
  {
    Set<String> supports();
  }

  interface ClassifierFactory<T> extends Query<T>
  {
    Classifier toClassifier(Sequence<T> sequence, String tagColumn);
  }

  interface WrappingQuery<T> extends Query<T>
  {
    Query query();

    WrappingQuery<T> withQuery(Query query);
  }

  interface LogProvider<T> extends Query<T>
  {
    Query<T> forLog();
  }

  // schema for sub-query handling
  interface SchemaProvider
  {
    RowSignature schema(QuerySegmentWalker segmentWalker);
  }
}
