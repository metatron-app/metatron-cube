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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import io.druid.common.DateTimes;
import io.druid.common.Intervals;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.StringUtils;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.select.ViewSupportHelper;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public abstract class BaseQuery<T> implements Query<T>
{
  @SuppressWarnings("unchecked")
  public static Query getRepresentative(Query query)
  {
    Query represent = query instanceof UnionAllQuery ? ((UnionAllQuery) query).getRepresentative() : query;
    if (represent != query) {
      return getRepresentative(represent);
    }
    return represent;
  }

  public static <T> boolean isLocalFinalizingQuery(Query<T> query)
  {
    return query.getContextBoolean(Query.FINAL_MERGE, true) || query.getContextBoolean(Query.FINALIZE, true);
  }

  public static <T> int getContextPriority(Query<T> query, int defaultValue)
  {
    return PropUtils.parseInt(query.getContext(), PRIORITY, defaultValue);
  }

  public static <T> boolean isBySegment(Query<T> query)
  {
    return query.getContextBoolean(BY_SEGMENT, false);
  }

  public static <T> boolean isPopulateCache(Query<T> query, boolean defaultValue)
  {
    return query.getContextBoolean(POPULATE_CACHE, defaultValue);
  }

  public static <T> boolean isUseCache(Query<T> query, boolean defaultValue)
  {
    return query.getContextBoolean(USE_CACHE, defaultValue);
  }

  public static <T> boolean isFinalize(Query<T> query, boolean defaultValue)
  {
    return PropUtils.parseBoolean(query.getContext(), FINALIZE, defaultValue);
  }

  public static <T> boolean isOptimizeQuery(Query<T> query, boolean defaultValue)
  {
    return PropUtils.parseBoolean(query.getContext(), OPTIMIZE_QUERY, defaultValue);
  }

  public static <T> boolean allDimensionsForEmpty(Query<T> query, boolean defaultValue)
  {
    return PropUtils.parseBoolean(query.getContext(), ALL_DIMENSIONS_FOR_EMPTY, defaultValue);
  }

  public static <T> boolean allMetricsForEmpty(Query<T> query, boolean defaultValue)
  {
    return PropUtils.parseBoolean(query.getContext(), ALL_METRICS_FOR_EMPTY, defaultValue);
  }

  public static <T> int getContextUncoveredIntervalsLimit(Query<T> query, int defaultValue)
  {
    return PropUtils.parseInt(query.getContext(), "uncoveredIntervalsLimit", defaultValue);
  }

  public static <T> String getResultForwardURL(Query<T> query)
  {
    return query.getContextValue(FORWARD_URL);
  }

  public static <T> Map<String, Object> getResultForwardContext(Query<T> query)
  {
    return query.getContextValue(FORWARD_CONTEXT, Maps.<String, Object>newHashMap());
  }

  public static <T> boolean isParallelForwarding(Query<T> query)
  {
    return !Strings.isNullOrEmpty(getResultForwardURL(query)) &&
           PropUtils.parseBoolean(getResultForwardContext(query), FORWARD_PARALLEL, false);
  }

  private final DataSource dataSource;
  private final boolean descending;
  private final Map<String, Object> context;
  private final QuerySegmentSpec querySegmentSpec;
  private volatile Duration duration;

  public BaseQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Map<String, Object> context
  )
  {
    this.dataSource = dataSource;
    this.querySegmentSpec = querySegmentSpec;
    this.descending = descending;
    this.context = context;
  }

  @JsonProperty
  @Override
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Override
  public boolean isDescending()
  {
    return descending;
  }

  @JsonProperty("intervals")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Override
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return querySegmentSpec;
  }

  @Override
  public boolean hasFilters()
  {
    if (this instanceof Query.FilterSupport) {
      if (((FilterSupport)this).getFilter() != null) {
        return true;
      }
    }
    return DataSources.hasFilter(getDataSource());
  }

  @Override
  public Granularity getGranularity()
  {
    return null;
  }

  public boolean allDimensionsForEmpty()
  {
    if (this instanceof AggregationsSupport) {
      // timeseries, group-by, top-n
      return BaseQuery.allDimensionsForEmpty(this, false);
    }
    // select, search, stream, summary, etc.
    return BaseQuery.allDimensionsForEmpty(this, true);
  }

  public boolean allMetricsForEmpty()
  {
    if (this instanceof AggregationsSupport) {
      return BaseQuery.allMetricsForEmpty(this, false);
    }
    return BaseQuery.allMetricsForEmpty(this, true);
  }

  public static List<VirtualColumn> getVirtualColumns(Query query)
  {
    return query instanceof VCSupport ? ((VCSupport<?>) query).getVirtualColumns() : ImmutableList.<VirtualColumn>of();
  }

  public static DimFilter getDimFilter(Query query)
  {
    return query instanceof Query.FilterSupport ? ((FilterSupport) query).getFilter() : null;
  }

  @SuppressWarnings("unchecked")
  public static List<DimensionSpec> getDimensions(Query query)
  {
    return query instanceof DimensionSupport ? ((DimensionSupport) query).getDimensions() : Arrays.asList();
  }

  public static List<String> getMetrics(Query query)
  {
    return query instanceof MetricSupport ? ((MetricSupport<?>) query).getMetrics() : Arrays.<String>asList();
  }

  @SuppressWarnings("unchecked")
  public static List<AggregatorFactory> getAggregators(Query query)
  {
    return query instanceof AggregationsSupport ? ((AggregationsSupport) query).getAggregatorSpecs() : Arrays.asList();
  }

  @SuppressWarnings("unchecked")
  public static List<PostAggregator> getPostAggregators(Query query)
  {
    return query instanceof AggregationsSupport ? ((AggregationsSupport) query).getPostAggregatorSpecs() : Arrays.asList();
  }

  @Override
  public Query<T> resolveQuery(Supplier<RowResolver> resolver, ObjectMapper mapper)
  {
    Query<T> query = ViewSupportHelper.rewrite(this, resolver);
    if (query instanceof AggregationsSupport) {
      AggregationsSupport<T> aggregationsSupport = (AggregationsSupport<T>) query;
      boolean changed = false;
      List<AggregatorFactory> resolved = Lists.newArrayList();
      for (AggregatorFactory factory : aggregationsSupport.getAggregatorSpecs()) {
        AggregatorFactory resolving = factory.resolveIfNeeded(resolver);
        if (factory != resolving) {
          changed = true;
        }
        resolved.add(resolving);
      }
      if (changed) {
        query = aggregationsSupport.withAggregatorSpecs(resolved);
      }
    }
    DimFilter filter = BaseQuery.getDimFilter(query);
    if (filter != null) {
      DimFilter optimized = DimFilters.convertToCNF(filter).optimize();
      Map<String, String> aliasMapping = QueryUtils.aliasMapping(this);
      if (!aliasMapping.isEmpty()) {
        optimized = optimized.withRedirection(aliasMapping);
      }
      if (filter != optimized) {
        query = ((FilterSupport<T>) query).withFilter(optimized);
      }
    }
    return query;
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context)
  {
    QuerySegmentSpec spec = querySegmentSpec == null ? MultipleIntervalSegmentSpec.ETERNITY : querySegmentSpec;
    QueryRunner<T> runner = spec.lookup(this, walker);
    if (walker instanceof ForwardingSegmentWalker) {
      runner = ((ForwardingSegmentWalker) walker).handle(this, runner);
    }
    return runner.run(this, assertContext(context));
  }

  public Map<String, Object> assertContext(Map<String, Object> context)
  {
    return context == null ? Maps.<String, Object>newHashMap() : context;
  }

  @Override
  public List<Interval> getIntervals()
  {
    return querySegmentSpec == null ? Intervals.ONLY_ETERNITY : querySegmentSpec.getIntervals();
  }

  @Override
  public Duration getDuration()
  {
    if (duration == null) {
      Duration totalDuration = new Duration(0);
      for (Interval interval : Iterables.filter(getIntervals(), Predicates.notNull())) {
        totalDuration = totalDuration.plus(interval.toDuration());
      }
      duration = totalDuration;
    }

    return duration;
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <ContextType> ContextType getContextValue(String key)
  {
    return context == null ? null : (ContextType) context.get(key);
  }

  @Override
  public <ContextType> ContextType getContextValue(String key, ContextType defaultValue)
  {
    ContextType retVal = getContextValue(key);
    return retVal == null ? defaultValue : retVal;
  }

  @Override
  public boolean getContextBoolean(String key, boolean defaultValue)
  {
    return PropUtils.parseBoolean(getContext(), key, defaultValue);
  }

  @Override
  public int getContextInt(String key, int defaultValue)
  {
    return PropUtils.parseInt(getContext(), key, defaultValue);
  }

  @Override
  public Query<T> withOverriddenContext(String contextKey, Object contextValue)
  {
    return withOverriddenContext(GuavaUtils.mutableMap(contextKey, contextValue));
  }

  @Override
  public Query<T> toLocalQuery()
  {
    return withOverriddenContext(defaultPostActionContext());
  }

  protected final Map<String, Object> defaultPostActionContext()
  {
    Map<String, Object> override = Maps.newHashMap();
    override.put(FINALIZE, false);
    override.put(FINAL_MERGE, false);
    override.put(POST_PROCESSING, null);
    return override;
  }

  public int getContextIntWithMax(String key, int defaultValue)
  {
    if (context != null && context.containsKey(key)) {
      return Math.min(getContextInt(key, defaultValue), defaultValue);
    }
    return defaultValue;
  }

  protected String toString(String key)
  {
    Object value = Objects.toString(getContextValue(key), null);
    return value == null ? "" : StringUtils.safeFormat(", %s=%s", key, value);
  }

  protected String toString(String... keys)
  {
    StringBuilder builder = new StringBuilder();
    for (String key : keys) {
      String value = toString(key);
      if (!Strings.isNullOrEmpty(value)) {
        builder.append(value);
      }
    }
    return builder.toString();
  }

  public static Map<String, Object> copyContext(Query<?> query)
  {
    return query.getContext() == null ? Maps.<String, Object>newHashMap() : Maps.newHashMap(query.getContext());
  }

  public static Map<String, Object> copyContextForMeta(Query<?> query)
  {
    return copyContextForMeta(query.getContext());
  }

  public static Map<String, Object> copyContextForMeta(Map<String, Object> context)
  {
    final Map<String, Object> forMeta = Maps.newHashMap();
    if (GuavaUtils.isNullOrEmpty(context)) {
      return forMeta;
    }
    for (String contextKey : Query.FOR_META) {
      if (context.containsKey(contextKey)) {
        forMeta.put(contextKey, context.get(contextKey));
      }
    }
    return forMeta;
  }

  public static Map<String, Object> contextRemover(String... keys)
  {
    Map<String, Object> remover = Maps.newHashMap();
    for (String key : keys) {
      remover.put(key, null);
    }
    return remover;
  }

  protected Map<String, Object> computeOverriddenContext(Map<String, Object> overrides)
  {
    return overrideContextWith(getContext(), overrides);
  }

  protected static Map<String, Object> overrideContextWith(Map<String, Object> context, Map<String, Object> overrides)
  {
    if (overrides == null) {
      return context;
    }
    Map<String, Object> overridden = Maps.newTreeMap();
    if (context != null) {
      overridden.putAll(context);
    }
    for (Map.Entry<String, Object> override : overrides.entrySet()) {
      if (StringUtils.isNullOrEmpty(override.getValue())) {
        overridden.remove(override.getKey());
      } else {
        overridden.put(override.getKey(), override.getValue());
      }
    }

    return overridden;
  }


  /**
   * If "query" has a single universal timestamp, return it. Otherwise return null. This is useful
   * for keeping timestamps in sync across partial queries that may have different intervals.
   *
   * @param query the query
   *
   * @return universal timestamp, or null
   */
  public static <T> Query<T> setUniversalTimestamp(Query<T> query)
  {
    final Number fudgeTimestamp = query.getContextValue(FUDGE_TIMESTAMP);
    if (fudgeTimestamp == null && Granularities.ALL.equals(query.getGranularity())) {
      final List<Interval> intervals = query.getIntervals();
      if (!intervals.isEmpty()) {
        return query.withOverriddenContext(FUDGE_TIMESTAMP, intervals.get(0).getStartMillis());
      }
    }
    return query;
  }

  public static <T> DateTime getUniversalTimestamp(Query<T> query)
  {
    final Number fudgeTimestamp = query.getContextValue(FUDGE_TIMESTAMP);
    if (fudgeTimestamp != null) {
      return DateTimes.utc(fudgeTimestamp.longValue());
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Ordering<T> getMergeOrdering()
  {
    Ordering<T> retVal = GuavaUtils.<T>noNullableNatural();
    return descending ? retVal.reverse() : retVal;
  }

  @Override
  public String getId()
  {
    return (String) getContextValue(QUERYID);
  }

  @Override
  public Query<T> withId(String id)
  {
    Preconditions.checkNotNull(id, "'id' should not be null");
    return withOverriddenContext(ImmutableMap.<String, Object>of(QUERYID, id));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseQuery baseQuery = (BaseQuery) o;

    if (!Objects.equals(dataSource, baseQuery.dataSource)) {
      return false;
    }
    if (descending != baseQuery.descending) {
      return false;
    }
    // removed context from eq/hash cause equality is just for test
    if (!Objects.equals(querySegmentSpec, baseQuery.querySegmentSpec)) {
      return false;
    }
    if (!Objects.equals(duration, baseQuery.duration)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dataSource.hashCode();
    result = 31 * result + (descending ? 1 : 0);
    result = 31 * result + (querySegmentSpec != null ? querySegmentSpec.hashCode() : 0);
    result = 31 * result + (duration != null ? duration.hashCode() : 0);
    return result;
  }
}
