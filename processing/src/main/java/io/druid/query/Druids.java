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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.NoopLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.metadata.metadata.ColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.query.search.search.FragmentSearchQuerySpec;
import io.druid.query.search.search.InsensitiveContainsSearchQuerySpec;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.query.search.search.SearchSortSpec;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.TableFunctionSpec;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 */
public class Druids
{
  public static final Function<String, DimensionSpec> DIMENSION_IDENTITY = new Function<String, DimensionSpec>()
  {
    @Nullable
    @Override
    public DimensionSpec apply(String input)
    {
      return new DefaultDimensionSpec(input, input);
    }
  };

  private Druids()
  {
    throw new AssertionError();
  }

  /**
   * A Builder for SelectorDimFilter.
   * <p/>
   * Required: dimension() and value() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   Selector selDimFilter = Druids.newSelectorDimFilterBuilder()
   *                                        .dimension("test")
   *                                        .value("sample")
   *                                        .build();
   * </code></pre>
   *
   * @see SelectorDimFilter
   */
  public static class SelectorDimFilterBuilder
  {
    private String dimension;
    private String value;

    public SelectorDimFilterBuilder()
    {
      dimension = "";
      value = "";
    }

    public SelectorDimFilter build()
    {
      return new SelectorDimFilter(dimension, value, null);
    }

    public SelectorDimFilterBuilder copy(SelectorDimFilterBuilder builder)
    {
      return new SelectorDimFilterBuilder()
          .dimension(builder.dimension)
          .value(builder.value);
    }

    public SelectorDimFilterBuilder dimension(String d)
    {
      dimension = d;
      return this;
    }

    public SelectorDimFilterBuilder value(String v)
    {
      value = v;
      return this;
    }
  }

  public static SelectorDimFilterBuilder newSelectorDimFilterBuilder()
  {
    return new SelectorDimFilterBuilder();
  }

  /**
   * A Builder for TimeseriesQuery.
   * <p/>
   * Required: dataSource(), intervals(), and aggregators() must be called before build()
   * Optional: filters(), granularity(), postAggregators(), and context() can be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
   *                                        .dataSource("Example")
   *                                        .intervals("2012-01-01/2012-01-02")
   *                                        .aggregators(listofAggregators)
   *                                        .build();
   * </code></pre>
   *
   * @see io.druid.query.timeseries.TimeseriesQuery
   */
  public static class TimeseriesQueryBuilder extends BaseAggregationQuery.Builder<TimeseriesQuery>
  {
    private TimeseriesQueryBuilder()
    {
      granularity = Granularities.ALL;
      aggregatorSpecs = Lists.newArrayList();
      postAggregatorSpecs = Lists.newArrayList();
    }

    @Override
    public TimeseriesQuery build()
    {
      Preconditions.checkArgument(GuavaUtils.isNullOrEmpty(dimensions), "cannot use dimensions in timeseries query");
      return new TimeseriesQuery(
          dataSource,
          querySegmentSpec,
          descending,
          dimFilter,
          granularity,
          virtualColumns,
          aggregatorSpecs,
          postAggregatorSpecs,
          havingSpec,
          buildLimitSpec(),
          outputColumns,
          lateralViewSpec,
          context
      );
    }

    public TimeseriesQueryBuilder copy(BaseAggregationQuery query)
    {
      return (TimeseriesQueryBuilder) new TimeseriesQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getIntervals())
          .filters(query.getFilter())
          .descending(query.isDescending())
          .granularity(query.getGranularity())
          .virtualColumns(query.getVirtualColumns())
          .aggregators(query.getAggregatorSpecs())
          .postAggregators(query.getPostAggregatorSpecs())
          .havingSpec(query.getHavingSpec())
          .limitSpec(query.getLimitSpec())
          .outputColumns(query.getOutputColumns())
          .lateralViewSpec(query.getLateralView())
          .context(query.getContext());
    }

    public TimeseriesQueryBuilder copy(TimeseriesQueryBuilder builder)
    {
      return (TimeseriesQueryBuilder) new TimeseriesQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .filters(builder.dimFilter)
          .descending(builder.descending)
          .granularity(builder.granularity)
          .virtualColumns(builder.virtualColumns)
          .aggregators(builder.aggregatorSpecs)
          .postAggregators(builder.postAggregatorSpecs)
          .havingSpec(builder.havingSpec)
          .limitSpec(builder.limitSpec)
          .outputColumns(builder.outputColumns)
          .lateralViewSpec(builder.lateralViewSpec)
          .context(builder.context);
    }

    public DataSource getDataSource()
    {
      return dataSource;
    }

    public QuerySegmentSpec getQuerySegmentSpec()
    {
      return querySegmentSpec;
    }

    public DimFilter getDimFilter()
    {
      return dimFilter;
    }

    public boolean isDescending()
    {
      return descending;
    }

    public Granularity getGranularity()
    {
      return granularity;
    }

    public List<AggregatorFactory> getAggregatorSpecs()
    {
      return aggregatorSpecs;
    }

    public List<PostAggregator> getPostAggregatorSpecs()
    {
      return postAggregatorSpecs;
    }

    public Map<String, Object> getContext()
    {
      return context;
    }
  }

  public static TimeseriesQueryBuilder newTimeseriesQueryBuilder()
  {
    return new TimeseriesQueryBuilder();
  }

  /**
   * A Builder for SearchQuery.
   * <p/>
   * Required: dataSource(), intervals(), dimensions() and query() must be called before build()
   * <p/>
   * Optional: filters(), granularity(), and context() can be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   SearchQuery query = Druids.newSearchQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .dimensions(listofEgDims)
   *                                  .query(exampleQuery)
   *                                  .intervals("2012-01-01/2012-01-02")
   *                                  .build();
   * </code></pre>
   *
   * @see io.druid.query.search.search.SearchQuery
   */
  public static class SearchQueryBuilder
  {
    private DataSource dataSource;
    private DimFilter dimFilter;
    private Granularity granularity;
    private int limit;
    private QuerySegmentSpec querySegmentSpec;
    private List<VirtualColumn> virtualColumns;
    private List<DimensionSpec> dimensions;
    private SearchQuerySpec querySpec;
    private SearchSortSpec sortSpec;
    private boolean valueOnly;
    private Map<String, Object> context;

    public SearchQueryBuilder()
    {
      dataSource = null;
      dimFilter = null;
      granularity = Granularities.ALL;
      limit = 0;
      querySegmentSpec = null;
      dimensions = null;
      querySpec = null;
      context = null;
    }

    public SearchQuery build()
    {
      return new SearchQuery(
          dataSource,
          dimFilter,
          granularity,
          limit,
          querySegmentSpec,
          virtualColumns,
          dimensions,
          querySpec,
          sortSpec,
          valueOnly,
          context
      );
    }

    public SearchQueryBuilder copy(SearchQuery query)
    {
      return new SearchQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getQuerySegmentSpec())
          .filters(query.getFilter())
          .granularity(query.getGranularity())
          .limit(query.getLimit())
          .dimensions(query.getDimensions())
          .query(query.getQuery())
          .context(query.getContext());
    }

    public SearchQueryBuilder copy(SearchQueryBuilder builder)
    {
      return new SearchQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .filters(builder.dimFilter)
          .granularity(builder.granularity)
          .limit(builder.limit)
          .dimensions(builder.dimensions)
          .query(builder.querySpec)
          .context(builder.context);
    }

    public SearchQueryBuilder dataSource(String d)
    {
      dataSource = new TableDataSource(d);
      return this;
    }

    public SearchQueryBuilder dataSource(DataSource d)
    {
      dataSource = d;
      return this;
    }

    public SearchQueryBuilder filters(String dimensionName, String value)
    {
      dimFilter = new SelectorDimFilter(dimensionName, value, null);
      return this;
    }

    public SearchQueryBuilder filters(String dimensionName, String value, String... values)
    {
      dimFilter = new InDimFilter(dimensionName, Lists.asList(value, values), null);
      return this;
    }

    public SearchQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
      return this;
    }

    public SearchQueryBuilder granularity(String g)
    {
      granularity = Granularity.fromString(g);
      return this;
    }

    public SearchQueryBuilder granularity(Granularity g)
    {
      granularity = g;
      return this;
    }

    public SearchQueryBuilder limit(int l)
    {
      limit = l;
      return this;
    }

    public SearchQueryBuilder valueOnly(boolean v)
    {
      valueOnly = v;
      return this;
    }

    public SearchQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public SearchQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public SearchQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public SearchQueryBuilder virtualColumns(VirtualColumn... vcs)
    {
      return virtualColumns(Arrays.asList(vcs));
    }

    public SearchQueryBuilder virtualColumns(List<VirtualColumn> vcs)
    {
      virtualColumns = vcs;
      return this;
    }

    public SearchQueryBuilder dimensions(String d)
    {
      dimensions = ImmutableList.of(DIMENSION_IDENTITY.apply(d));
      return this;
    }

    public SearchQueryBuilder dimensions(Iterable<String> d)
    {
      dimensions = ImmutableList.copyOf(Iterables.transform(d, DIMENSION_IDENTITY));
      return this;
    }

    public SearchQueryBuilder dimensions(DimensionSpec d)
    {
      dimensions = Lists.newArrayList(d);
      return this;
    }

    public SearchQueryBuilder dimensions(List<DimensionSpec> d)
    {
      dimensions = d;
      return this;
    }

    public SearchQueryBuilder query(SearchQuerySpec s)
    {
      querySpec = s;
      return this;
    }

    public SearchQueryBuilder query(String q)
    {
      Preconditions.checkNotNull(q, "no value");
      querySpec = new InsensitiveContainsSearchQuerySpec(q);
      return this;
    }

    public SearchQueryBuilder query(Map<String, Object> q)
    {
      String value = Preconditions.checkNotNull(q.get("value"), "no value").toString();
      querySpec = new InsensitiveContainsSearchQuerySpec(value);
      return this;
    }

    public SearchQueryBuilder query(String q, boolean caseSensitive)
    {
      Preconditions.checkNotNull(q, "no value");
      querySpec = new ContainsSearchQuerySpec(q, caseSensitive);
      return this;
    }

    public SearchQueryBuilder query(Map<String, Object> q, boolean caseSensitive)
    {
      String value = Preconditions.checkNotNull(q.get("value"), "no value").toString();
      querySpec = new ContainsSearchQuerySpec(value, caseSensitive);
      return this;
    }

    public SearchQueryBuilder fragments(List<String> q)
    {
      return fragments(q, false);
    }

    public SearchQueryBuilder sortSpec(SearchSortSpec sortSpec)
    {
      this.sortSpec = sortSpec;
      return this;
    }

    public SearchQueryBuilder fragments(List<String> q, boolean caseSensitive)
    {
      Preconditions.checkNotNull(q, "no value");
      querySpec = new FragmentSearchQuerySpec(q, caseSensitive);
      return this;
    }

    public SearchQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }
  }

  public static SearchQueryBuilder newSearchQueryBuilder()
  {
    return new SearchQueryBuilder();
  }

  /**
   * A Builder for TimeBoundaryQuery.
   * <p/>
   * Required: dataSource() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   TimeBoundaryQuery query = new MaxTimeQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .build();
   * </code></pre>
   *
   * @see io.druid.query.timeboundary.TimeBoundaryQuery
   */
  public static class TimeBoundaryQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private String bound;
    private Map<String, Object> context;

    public TimeBoundaryQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      bound = null;
      context = null;
    }

    public TimeBoundaryQuery build()
    {
      return new TimeBoundaryQuery(
          dataSource,
          querySegmentSpec,
          bound,
          context
      );
    }

    public TimeBoundaryQueryBuilder copy(TimeBoundaryQueryBuilder builder)
    {
      return new TimeBoundaryQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .bound(builder.bound)
          .context(builder.context);
    }

    public TimeBoundaryQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public TimeBoundaryQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public TimeBoundaryQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public TimeBoundaryQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public TimeBoundaryQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public TimeBoundaryQueryBuilder bound(String b)
    {
      bound = b;
      return this;
    }

    public TimeBoundaryQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }
  }

  public static TimeBoundaryQueryBuilder newTimeBoundaryQueryBuilder()
  {
    return new TimeBoundaryQueryBuilder();
  }

  /**
   * A Builder for SegmentMetadataQuery.
   * <p/>
   * Required: dataSource(), intervals() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   SegmentMetadataQuery query = new SegmentMetadataQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .interval("2010/2013")
   *                                  .build();
   * </code></pre>
   *
   * @see io.druid.query.metadata.metadata.SegmentMetadataQuery
   */
  public static class SegmentMetadataQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private List<VirtualColumn> virtualColumns;
    private ColumnIncluderator toInclude;
    private List<String> columns;
    private EnumSet<SegmentMetadataQuery.AnalysisType> analysisTypes;
    private Boolean merge;
    private Boolean lenientAggregatorMerge;
    private Map<String, Object> context;

    public SegmentMetadataQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      toInclude = null;
      analysisTypes = null;
      merge = null;
      context = null;
      lenientAggregatorMerge = null;
    }

    public SegmentMetadataQuery build()
    {
      return new SegmentMetadataQuery(
          dataSource,
          querySegmentSpec,
          false,
          virtualColumns,
          toInclude,
          columns,
          merge,
          analysisTypes,
          false,
          lenientAggregatorMerge,
          context
      );
    }

    public SegmentMetadataQueryBuilder copy(SegmentMetadataQueryBuilder builder)
    {
      final SegmentMetadataQuery.AnalysisType[] analysisTypesArray =
          analysisTypes != null
          ? analysisTypes.toArray(new SegmentMetadataQuery.AnalysisType[0])
          : null;
      return new SegmentMetadataQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .toInclude(toInclude)
          .analysisTypes(analysisTypesArray)
          .merge(merge)
          .lenientAggregatorMerge(lenientAggregatorMerge)
          .context(builder.context);
    }

    public SegmentMetadataQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public SegmentMetadataQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public SegmentMetadataQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public SegmentMetadataQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public SegmentMetadataQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public SegmentMetadataQueryBuilder virtualColumns(List<VirtualColumn> virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public SegmentMetadataQueryBuilder toInclude(ColumnIncluderator toInclude)
    {
      this.toInclude = toInclude;
      return this;
    }

    public SegmentMetadataQueryBuilder columns(List<String> columns)
    {
      this.columns = columns;
      return this;
    }

    public SegmentMetadataQueryBuilder analysisTypes(SegmentMetadataQuery.AnalysisType... analysisTypes)
    {
      if (analysisTypes == null) {
        this.analysisTypes = null;
      } else {
        this.analysisTypes = analysisTypes.length == 0
                             ? EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class)
                             : EnumSet.copyOf(Arrays.asList(analysisTypes));
      }
      return this;
    }

    public SegmentMetadataQueryBuilder merge(boolean merge)
    {
      this.merge = merge;
      return this;
    }

    public SegmentMetadataQueryBuilder lenientAggregatorMerge(boolean lenientAggregatorMerge)
    {
      this.lenientAggregatorMerge = lenientAggregatorMerge;
      return this;
    }

    public SegmentMetadataQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }
  }

  public static SegmentMetadataQueryBuilder newSegmentMetadataQueryBuilder()
  {
    return new SegmentMetadataQueryBuilder();
  }

  /**
   * A Builder for SelectQuery.
   * <p/>
   * Required: dataSource(), intervals() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   SelectQuery query = new SelectQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .interval("2010/2013")
   *                                  .build();
   * </code></pre>
   *
   * @see io.druid.query.select.SelectQuery
   */
  public static class SelectQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private boolean descending;
    private Map<String, Object> context;
    private DimFilter dimFilter;
    private Granularity granularity;
    private TableFunctionSpec tableFunction;
    private List<DimensionSpec> dimensions;
    private List<String> metrics;
    private List<String> columns;
    private List<VirtualColumn> virtualColumns;
    private List<OrderByColumnSpec> orderingSpecs;
    private PagingSpec pagingSpec;
    private LimitSpec limitSpec = NoopLimitSpec.INSTANCE;
    private String concatString;
    private LateralViewSpec lateralViewSpec;
    private List<String> outputColumns;

    public SelectQueryBuilder()
    {
      granularity = Granularities.ALL;
      dimensions = Lists.newArrayList();
      metrics = Lists.newArrayList();
    }

    public StreamQuery streaming()
    {
      return streaming(null);
    }

    public StreamQuery streaming(List<String> sortOn)
    {
      Preconditions.checkArgument(GuavaUtils.isNullOrEmpty(dimensions));
      Preconditions.checkArgument(GuavaUtils.isNullOrEmpty(metrics));
      Preconditions.checkArgument(pagingSpec == null || GuavaUtils.isNullOrEmpty(pagingSpec.getPagingIdentifiers()));
      if (!GuavaUtils.isNullOrEmpty(sortOn)) {
        if (limitSpec.isNoop()) {
          orderingSpecs = OrderByColumnSpec.ascending(sortOn);
        } else {
          limitSpec = limitSpec.withOrderingSpec(OrderByColumnSpec.ascending(sortOn));
        }
      }
      return new StreamQuery(
          dataSource,
          querySegmentSpec,
          descending,
          dimFilter,
          tableFunction,
          columns,
          virtualColumns,
          orderingSpecs,
          concatString,
          limitSpec,
          outputColumns,
          context
      );
    }

    public SelectQuery build()
    {
      Preconditions.checkArgument(GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs()));
      Preconditions.checkArgument(GuavaUtils.isNullOrEmpty(limitSpec.getColumns()));
      Preconditions.checkArgument(GuavaUtils.isNullOrEmpty(columns));
      return new SelectQuery(
          dataSource,
          querySegmentSpec,
          descending,
          dimFilter,
          granularity,
          dimensions,
          metrics,
          virtualColumns,
          pagingSpec,
          concatString,
          outputColumns,
          lateralViewSpec,
          context
      );
    }

    public SelectQueryBuilder copy(SelectQueryBuilder builder)
    {
      return new SelectQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .descending(builder.descending)
          .filters(builder.dimFilter)
          .granularity(builder.granularity)
          .dimensionSpecs(builder.dimensions)
          .metrics(builder.metrics)
          .virtualColumns(builder.virtualColumns)
          .pagingSpec(builder.pagingSpec)
          .lateralViewSpec(builder.lateralViewSpec)
          .outputColumns(builder.outputColumns)
          .context(builder.context);
    }

    public static SelectQueryBuilder copy(SelectQuery query)
    {
      return new SelectQueryBuilder()
          .dataSource(query.getDataSource())
          .intervals(query.getIntervals())
          .descending(query.isDescending())
          .filters(query.getFilter())
          .granularity(query.getGranularity())
          .dimensionSpecs(query.getDimensions())
          .metrics(query.getMetrics())
          .virtualColumns(query.getVirtualColumns())
          .pagingSpec(query.getPagingSpec())
          .lateralViewSpec(query.getLateralView())
          .outputColumns(query.getOutputColumns())
          .context(query.getContext());
    }

    public SelectQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public SelectQueryBuilder dataSource(Query query)
    {
      dataSource = QueryDataSource.of(query);
      return this;
    }

    public SelectQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public SelectQueryBuilder interval(Interval interval)
    {
      querySegmentSpec = new LegacySegmentSpec(interval);
      return this;
    }

    public SelectQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public SelectQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public SelectQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new MultipleIntervalSegmentSpec(l);
      return this;
    }

    public SelectQueryBuilder intervals(Interval interval)
    {
      return intervals(Arrays.asList(interval));
    }

    public SelectQueryBuilder descending(boolean descending)
    {
      this.descending = descending;
      return this;
    }

    public SelectQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }

    public SelectQueryBuilder addContext(String key, Object value)
    {
      if (context == null) {
        context = Maps.newHashMap();
      } else {
        context = Maps.newHashMap(context);
      }
      context.put(key, value);
      return this;
    }

    public SelectQueryBuilder addContext(Map<String, Object> c)
    {
      if (context == null) {
        context = c;
      } else {
        context = Maps.newHashMap(context);
        context.putAll(c);
      }
      return this;
    }

    public SelectQueryBuilder filters(String dimensionName, String value)
    {
      dimFilter = new SelectorDimFilter(dimensionName, value, null);
      return this;
    }

    public SelectQueryBuilder filters(String dimensionName, String value, String... values)
    {
      dimFilter = new InDimFilter(dimensionName, Lists.asList(value, values), null);
      return this;
    }

    public SelectQueryBuilder filters(DimFilter f)
    {
      dimFilter = f;
      return this;
    }

    public SelectQueryBuilder granularity(String g)
    {
      granularity = Granularity.fromString(g);
      return this;
    }

    public SelectQueryBuilder granularity(Granularity g)
    {
      granularity = g;
      return this;
    }

    public SelectQueryBuilder dimensionSpecs(List<DimensionSpec> d)
    {
      dimensions = d;
      return this;
    }

    public SelectQueryBuilder dimensionSpecs(DimensionSpec... d)
    {
      dimensions = Arrays.asList(d);
      return this;
    }

    public SelectQueryBuilder dimensions(String... d)
    {
      return dimensions(Arrays.asList(d));
    }

    public SelectQueryBuilder dimensions(List<String> d)
    {
      dimensions = DefaultDimensionSpec.toSpec(d);
      return this;
    }

    public SelectQueryBuilder columns(List<String> c)
    {
      columns = c;
      return this;
    }

    public SelectQueryBuilder columns(String... c)
    {
      columns = Arrays.asList(c);
      return this;
    }

    public SelectQueryBuilder metrics(String... m)
    {
      return metrics(Arrays.asList(m));
    }

    public SelectQueryBuilder metrics(List<String> m)
    {
      metrics = m;
      return this;
    }

    public SelectQueryBuilder virtualColumns(List<VirtualColumn> vcs)
    {
      virtualColumns = vcs;
      return this;
    }

    public SelectQueryBuilder virtualColumns(VirtualColumn... vcs)
    {
      virtualColumns = Arrays.asList(vcs);
      return this;
    }

    public SelectQueryBuilder pagingSpec(PagingSpec p)
    {
      pagingSpec = p;
      return this;
    }

    public SelectQueryBuilder concatString(String c)
    {
      concatString = c;
      return this;
    }

    public SelectQueryBuilder lateralViewSpec(LateralViewSpec e)
    {
      lateralViewSpec = e;
      return this;
    }

    public SelectQueryBuilder outputColumns(String... o)
    {
      return outputColumns(Arrays.asList(o));
    }

    public SelectQueryBuilder outputColumns(List<String> o)
    {
      outputColumns = o;
      return this;
    }

    public SelectQueryBuilder limit(int limit)
    {
      pagingSpec = PagingSpec.newSpec(limit);
      limitSpec = limitSpec.withLimit(limit);
      return this;
    }

    public SelectQueryBuilder limitSpec(LimitSpec l)
    {
      limitSpec = l;
      return this;
    }

    public SelectQueryBuilder orderBy(OrderByColumnSpec... orderBy)
    {
      limitSpec = limitSpec.withOrderingSpec(Arrays.asList(orderBy));
      return this;
    }

    public SelectQueryBuilder orderBy(List<OrderByColumnSpec> orderBy)
    {
      limitSpec = limitSpec.withOrderingSpec(orderBy);
      return this;
    }
  }

  public static SelectQueryBuilder newSelectQueryBuilder()
  {
    return new SelectQueryBuilder();
  }

  /**
   * A Builder for DataSourceMetadataQuery.
   * <p/>
   * Required: dataSource() must be called before build()
   * <p/>
   * Usage example:
   * <pre><code>
   *   DataSourceMetadataQueryBuilder query = new DataSourceMetadataQueryBuilder()
   *                                  .dataSource("Example")
   *                                  .build();
   * </code></pre>
   *
   * @see io.druid.query.datasourcemetadata.DataSourceMetadataQuery
   */
  public static class DataSourceMetadataQueryBuilder
  {
    private DataSource dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private Map<String, Object> context;

    public DataSourceMetadataQueryBuilder()
    {
      dataSource = null;
      querySegmentSpec = null;
      context = null;
    }

    public DataSourceMetadataQuery build()
    {
      return new DataSourceMetadataQuery(
          dataSource,
          querySegmentSpec,
          context
      );
    }

    public DataSourceMetadataQueryBuilder copy(DataSourceMetadataQueryBuilder builder)
    {
      return new DataSourceMetadataQueryBuilder()
          .dataSource(builder.dataSource)
          .intervals(builder.querySegmentSpec)
          .context(builder.context);
    }

    public DataSourceMetadataQueryBuilder dataSource(String ds)
    {
      dataSource = new TableDataSource(ds);
      return this;
    }

    public DataSourceMetadataQueryBuilder dataSource(DataSource ds)
    {
      dataSource = ds;
      return this;
    }

    public DataSourceMetadataQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public DataSourceMetadataQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public DataSourceMetadataQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public DataSourceMetadataQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }
  }

  public static DataSourceMetadataQueryBuilder newDataSourceMetadataQueryBuilder()
  {
    return new DataSourceMetadataQueryBuilder();
  }

  public static class JoinQueryBuilder
  {
    private Map<String, DataSource> dataSources = Maps.newHashMap();
    private QuerySegmentSpec querySegmentSpec;
    private List<JoinElement> elements = Lists.newArrayList();
    private boolean prefixAlias;
    private boolean asMap;
    private String timeColumnName;
    private int limit;
    private int maxOutputRow;
    private List<String> outputAlias;
    private List<String> outputColumns;
    private RowSignature signature;
    private Map<String, Object> context = Maps.newHashMap();

    public JoinQuery build()
    {
      final Map<String, DataSource> normalize = JoinQuery.normalize(dataSources);
      final List<JoinElement> validated = JoinQuery.validateElements(normalize, elements);
      return new JoinQuery(
          normalize,
          querySegmentSpec,
          validated,
          prefixAlias,
          asMap,
          timeColumnName,
          limit,
          maxOutputRow,
          outputAlias,
          outputColumns,
          context
      ).withSchema(signature);
    }

    public JoinQueryBuilder dataSources(Map<String, DataSource> dataSources)
    {
      this.dataSources = Maps.newHashMap(dataSources);
      return this;
    }

    public JoinQueryBuilder dataSource(String alias, DataSource ds)
    {
      dataSources.put(alias, ds);
      return this;
    }

    public JoinQueryBuilder dataSource(String alias, String table)
    {
      return dataSource(alias, TableDataSource.of(table));
    }

    public JoinQueryBuilder dataSource(String alias, Query query)
    {
      return dataSource(alias, QueryDataSource.of(query));
    }

    public JoinQueryBuilder interval(Interval interval)
    {
      querySegmentSpec = new LegacySegmentSpec(interval);
      return this;
    }

    public JoinQueryBuilder intervals(QuerySegmentSpec q)
    {
      querySegmentSpec = q;
      return this;
    }

    public JoinQueryBuilder intervals(String s)
    {
      querySegmentSpec = new LegacySegmentSpec(s);
      return this;
    }

    public JoinQueryBuilder intervals(List<Interval> l)
    {
      querySegmentSpec = new LegacySegmentSpec(l);
      return this;
    }

    public JoinQueryBuilder elements(List<JoinElement> elements)
    {
      this.elements = elements;
      return this;
    }

    public JoinQueryBuilder element(JoinElement element)
    {
      elements.add(element);
      return this;
    }

    public JoinQueryBuilder context(Map<String, Object> c)
    {
      context = c;
      return this;
    }

    public JoinQueryBuilder context(String key, Object value)
    {
      context.put(key, value);
      return this;
    }

    public JoinQueryBuilder addContext(String key, Object value)
    {
      context.put(key, value);
      return this;
    }

    public JoinQueryBuilder addContext(Map<String, Object> c)
    {
      if (context == null) {
        context = c;
      } else {
        context = Maps.newHashMap(context);
        context.putAll(c);
      }
      return this;
    }

    public JoinQueryBuilder prefixAlias(boolean prefixAlias)
    {
      this.prefixAlias = prefixAlias;
      return this;
    }

    public JoinQueryBuilder asMap(boolean asMap)
    {
      this.asMap = asMap;
      return this;
    }

    public JoinQueryBuilder timeColumnName(String timeColumnName)
    {
      this.timeColumnName = timeColumnName;
      return this;
    }

    public JoinQueryBuilder limit(int limit)
    {
      this.limit = limit;
      return this;
    }

    public JoinQueryBuilder maxOutputRow(int maxOutputRow)
    {
      this.maxOutputRow = maxOutputRow;
      return this;
    }

    public JoinQueryBuilder outputAlias(String... outputAlias)
    {
      return outputAlias(Arrays.asList(outputAlias));
    }

    public JoinQueryBuilder outputAlias(List<String> outputAlias)
    {
      this.outputAlias = outputAlias;
      return this;
    }

    public JoinQueryBuilder outputColumns(String... outputColumns)
    {
      return outputColumns(Arrays.asList(outputColumns));
    }

    public JoinQueryBuilder outputColumns(List<String> outputColumns)
    {
      this.outputColumns = outputColumns;
      return this;
    }

    public JoinQueryBuilder withSchema(RowSignature signature)
    {
      this.signature = signature;
      return this;
    }
  }

  public static JoinQueryBuilder newJoinQueryBuilder()
  {
    return new JoinQueryBuilder();
  }

  public static BaseAggregationQuery.Builder builderFor(BaseAggregationQuery query)
  {
    if (query instanceof GroupByQuery) {
      return new GroupByQuery.Builder((GroupByQuery) query);
    } else if (query instanceof TimeseriesQuery) {
      return new TimeseriesQuery.Builder((TimeseriesQuery) query);
    }
    throw new UnsupportedOperationException("?? " + query.getClass());
  }
}
