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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.Row;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.Pair;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.GroupByMetaQuery;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.kmeans.FindNearestQuery;
import io.druid.query.kmeans.KMeansQuery;
import io.druid.query.kmeans.KMeansTaggingQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.select.SelectForwardQuery;
import io.druid.query.select.SelectMetaQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.TableFunctionSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.VirtualColumn;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
    @JsonSubTypes.Type(name = Query.SELECT_DELEGATE, value = SelectForwardQuery.class),
    @JsonSubTypes.Type(name = Query.TOPN, value = TopNQuery.class),
    @JsonSubTypes.Type(name = Query.DATASOURCE_METADATA, value = DataSourceMetadataQuery.class),
    @JsonSubTypes.Type(name = Query.UNION_ALL, value = UnionAllQuery.class),
    @JsonSubTypes.Type(name = Query.JOIN, value = JoinQuery.class),
    @JsonSubTypes.Type(name = Query.FILTER_META, value = FilterMetaQuery.class),
    @JsonSubTypes.Type(name = Query.DIMENSION_SAMPLING, value = DimensionSamplingQuery.class),
    @JsonSubTypes.Type(name = Query.CARDINALITY_META, value = CardinalityMetaQuery.class),
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
  String JOIN = "join";
  String FILTER_META = "$filter.meta";    // internal query
  String DIMENSION_SAMPLING = "$dimension.sampling";  // internal query
  String CARDINALITY_META = "$cardinality.meta";  // internal query

  DataSource getDataSource();

  boolean hasFilters();

  Granularity getGranularity();

  String getType();

  Query<T> resolveQuery(Supplier<RowResolver> resolver, boolean expand);

  Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context);

  QueryRunner<T> makeQueryRunner(QuerySegmentWalker walker);

  List<Interval> getIntervals();

  QuerySegmentSpec getQuerySegmentSpec();

  Duration getDuration();

  Map<String, Object> getContext();

  boolean hasContext(String key);

  <ContextType> ContextType getContextValue(String key);

  <ContextType> ContextType getContextValue(String key, ContextType defaultValue);

  boolean getContextBoolean(String key, boolean defaultValue);

  int getContextInt(String key, int defaultValue);

  long getContextLong(String key, long defaultValue);

  default float getContextFloat(String key, float defaultValue)
  {
    return (float) getContextDouble(key, defaultValue);
  }

  double getContextDouble(String key, double defaultValue);

  boolean isDescending();

  // used for merging partial results.. return null if no need to (concat all: see stream query)
  Comparator<T> getMergeOrdering(List<String> columns);

  Query<T> withOverriddenContext(Map<String, Object> contextOverride);

  default Query<T> withOverriddenContext(String contextKey, Object contextValue)
  {
    return withOverriddenContext(
        contextValue == null ? GuavaUtils.mutableMap(contextKey, contextValue) :
        ImmutableMap.of(contextKey, contextValue)
    );
  }

  Query<T> withQuerySegmentSpec(QuerySegmentSpec spec);

  Query<T> toLocalQuery();

  Query<T> withId(String id);

  default Query<T> withRandomId()
  {
    return withId(UUID.randomUUID().toString());
  }

  String getId();

  default String alias()
  {
    if (this instanceof JoinQuery.JoinHolder) {
      return ((JoinQuery.JoinHolder) this).getAlias();
    }
    DataSource dataSource = getDataSource();
    if (dataSource instanceof TableDataSource) {
      return ((TableDataSource) dataSource).getName();
    }
    if (dataSource instanceof ViewDataSource) {
      return ((ViewDataSource) dataSource).getName();
    }
    return StringUtils.concat(",", dataSource.getNames());
  }

  Query<T> withDataSource(DataSource dataSource);

  @JsonIgnore
  default List<String> estimatedInitialColumns()
  {
    return null;
  }

  @JsonIgnore
  default List<String> estimatedOutputColumns()
  {
    return null;
  }

  interface VCSupport<T> extends Query<T>
  {
    List<VirtualColumn> getVirtualColumns();

    VCSupport<T> withVirtualColumns(List<VirtualColumn> virtualColumns);
  }

  interface FilterSupport<T> extends VCSupport<T>
  {
    DimFilter getFilter();

    FilterSupport<T> withFilter(DimFilter filter);

    FilterSupport<T> withVirtualColumns(List<VirtualColumn> virtualColumns);
  }

  interface ColumnsSupport<T> extends FilterSupport<T>
  {
    TableFunctionSpec getTableFunction();

    ColumnsSupport<T> withTableFunction(TableFunctionSpec tableFunction);

    List<String> getColumns();

    ColumnsSupport<T> withColumns(List<String> columns);

    ColumnsSupport<T> withFilter(DimFilter filter);

    ColumnsSupport<T> withVirtualColumns(List<VirtualColumn> virtualColumns);
  }

  interface DimensionSupport<T> extends FilterSupport<T>
  {
    List<DimensionSpec> getDimensions();

    DimensionSupport<T> withDimensionSpecs(List<DimensionSpec> dimensions);

    DimensionSupport<T> withFilter(DimFilter filter);

    DimensionSupport<T> withVirtualColumns(List<VirtualColumn> virtualColumns);

    boolean allDimensionsForEmpty();
  }

  interface MetricSupport<T> extends DimensionSupport<T>
  {
    List<String> getMetrics();

    MetricSupport<T> withMetrics(List<String> metrics);

    MetricSupport<T> withFilter(DimFilter filter);

    MetricSupport<T> withVirtualColumns(List<VirtualColumn> virtualColumns);

    boolean allMetricsForEmpty();
  }

  interface AggregationsSupport<T> extends DimensionSupport<T>, ArrayOutputSupport<T>
  {
    List<AggregatorFactory> getAggregatorSpecs();

    AggregationsSupport<T> withAggregatorSpecs(List<AggregatorFactory> metrics);

    List<PostAggregator> getPostAggregatorSpecs();

    AggregationsSupport<T> withPostAggregatorSpecs(List<PostAggregator> metrics);

    AggregationsSupport<T> withFilter(DimFilter filter);

    default AggregationsSupport<T> prepend(DimFilter filter)
    {
      return withFilter(DimFilters.and(filter, getFilter()));
    }

    AggregationsSupport<T> withVirtualColumns(List<VirtualColumn> virtualColumns);

    boolean allMetricsForEmpty();
  }

  interface ArrayOutputSupport<T> extends Query<T>
  {
    Sequence<Object[]> array(Sequence<T> sequence);
  }

  interface LastProjectionSupport<T> extends Query<T>
  {
    List<String> getOutputColumns();

    LastProjectionSupport<T> withOutputColumns(List<String> outputColumns);
  }

  interface ArrayOutput extends ArrayOutputSupport<Object[]>
  {
    default Sequence<Object[]> array(Sequence<Object[]> sequence)
    {
      return sequence;
    }
  }

  interface RowOutputSupport<T> extends Query<T>
  {
    Sequence<Row> asRow(Sequence<T> sequence);
  }

  interface MapOutputSupport<T> extends Query<T>
  {
    Sequence<Map<String, Object>> asMap(Sequence<T> sequence);
  }

  interface RowOutput extends RowOutputSupport<Row>
  {
    default Sequence<Row> asRow(Sequence<Row> sequence) { return sequence; }
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

  interface LimitSupport<T> extends Query<T>
  {
    LimitSpec getLimitSpec();

    LimitSupport<T> withLimitSpec(LimitSpec limitSpec);
  }

  interface RewritingQuery<T> extends Query<T>
  {
    Query rewriteQuery(QuerySegmentWalker segmentWalker);
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

  // for iteration.. (see Queries.iterate)
  interface WrappingQuery<T> extends Query<T>
  {
    List<Query> getQueries();

    WrappingQuery<T> withQueries(List<Query> queries);
  }

  interface LogProvider<T> extends Query<T>
  {
    Query<T> forLog();
  }

  interface SchemaHolder
  {
    RowSignature schema();
  }

  // schema for sub-query handling
  interface SchemaProvider
  {
    RowSignature schema(QuerySegmentWalker segmentWalker);
  }

  interface Cacheable<T> extends Query<T>, io.druid.common.Cacheable
  {
    @Override
    default byte[] getCacheKey()
    {
      if (!(getDataSource() instanceof TableDataSource)) {
        return null;
      }
      if (getContextValue(Query.POST_PROCESSING) != null || getContextValue(Query.LOCAL_POST_PROCESSING) != null) {
        return null;
      }
      return getCacheKey(KeyBuilder.get(DEFAULT_KEY_LIMIT)).build();
    }
  }
}
