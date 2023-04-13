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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import io.druid.common.Intervals;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.Comparators;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.output.ForwardConstants;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public abstract class BaseQuery<T> implements Query<T>
{
  public static Query getRepresentative(Query query)
  {
    Query represent = query instanceof UnionAllQuery ? ((UnionAllQuery) query).getRepresentative() : query;
    if (represent != query) {
      return getRepresentative(represent);
    }
    return represent;
  }

  public static boolean isLocalFinalizingQuery(Query<?> query)
  {
    return isBrokerSide(query) || isFinalize(query);    // ??
  }

  public static int getContextPriority(Query<?> query, int defaultValue)
  {
    return PropUtils.parseInt(query.getContext(), PRIORITY, defaultValue);
  }

  public static <T> Query<T> enforceTimeout(Query<T> query, long maxTimeout)
  {
    final Long timeout = getTimeout(query);
    if (timeout == null || timeout > maxTimeout) {
      return query.withOverriddenContext(TIMEOUT, maxTimeout);
    }
    return query;
  }

  public static long getTimeout(Query<?> query, long maxTimeout)
  {
    final Long timeout = getTimeout(query);
    if (timeout == null || timeout > maxTimeout) {
      return maxTimeout;
    }
    return timeout;
  }

  private static Long getTimeout(Query<?> query)
  {
    final Object value = query.getContextValue(TIMEOUT);
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Longs.tryParse((String) value);
    }
    return null;
  }

  public static boolean isBySegment(Query<?> query)
  {
    return query.getContextBoolean(BY_SEGMENT, false);
  }

  public static boolean isUseCache(Query<?> query, boolean defaultValue)
  {
    return query.getContextBoolean(USE_CACHE, defaultValue);
  }

  public static boolean isPopulateCache(Query<?> query, boolean defaultValue)
  {
    return query.getContextBoolean(POPULATE_CACHE, defaultValue);
  }

  public static boolean isFinalize(Query<?> query)
  {
    return PropUtils.parseBoolean(query.getContext(), FINALIZE, true);
  }

  public static boolean isBrokerSide(Query<?> query)
  {
    return PropUtils.parseBoolean(query.getContext(), BROKER_SIDE, true);
  }

  public static boolean allDimensionsForEmpty(Query<?> query, boolean defaultValue)
  {
    return PropUtils.parseBoolean(query.getContext(), ALL_DIMENSIONS_FOR_EMPTY, defaultValue);
  }

  public static boolean allMetricsForEmpty(Query<?> query, boolean defaultValue)
  {
    return PropUtils.parseBoolean(query.getContext(), ALL_METRICS_FOR_EMPTY, defaultValue);
  }

  public static int getContextUncoveredIntervalsLimit(Query<?> query, int defaultValue)
  {
    return PropUtils.parseInt(query.getContext(), "uncoveredIntervalsLimit", defaultValue);
  }

  public static String getResultForwardURL(Query<?> query)
  {
    return query.getContextValue(FORWARD_URL);
  }

  public static Map<String, Object> getResultForwardContext(Query<?> query)
  {
    return query.getContextValue(FORWARD_CONTEXT, Maps.<String, Object>newHashMap());
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> removeDecoratorContext(Query<?> query)
  {
    return (Map<String, Object>) query.getContext().remove(DECORATOR_CONTEXT);
  }

  public static boolean isParallelForwarding(Query<?> query)
  {
    return !Strings.isNullOrEmpty(getResultForwardURL(query)) &&
           PropUtils.parseBoolean(getResultForwardContext(query), ForwardConstants.PARALLEL, false);
  }

  protected final DataSource dataSource;
  protected final boolean descending;
  protected final Map<String, Object> context;
  protected final QuerySegmentSpec querySegmentSpec;
  protected volatile Duration duration;

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
      if (((FilterSupport) this).getFilter() != null) {
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
    return query instanceof VCSupport ? ((VCSupport<?>) query).getVirtualColumns() : Collections.emptyList();
  }

  public static DimFilter getDimFilter(Query query)
  {
    return query instanceof Query.FilterSupport ? ((FilterSupport) query).getFilter() : null;
  }

  public static List<DimensionSpec> getDimensions(Query query)
  {
    return query instanceof DimensionSupport ? ((DimensionSupport<?>) query).getDimensions() : Collections.emptyList();
  }

  public static List<String> getMetrics(Query query)
  {
    return query instanceof MetricSupport ? ((MetricSupport<?>) query).getMetrics() : Collections.emptyList();
  }

  public static List<AggregatorFactory> getAggregators(Query query)
  {
    return query instanceof AggregationsSupport
           ? ((AggregationsSupport<?>) query).getAggregatorSpecs() : Collections.emptyList();
  }

  public static List<PostAggregator> getPostAggregators(Query query)
  {
    return query instanceof AggregationsSupport
           ? ((AggregationsSupport<?>) query).getPostAggregatorSpecs() : Collections.emptyList();
  }

  public static LimitSpec getLimitSpec(Query query)
  {
    return query instanceof LimitSupport ? ((LimitSupport) query).getLimitSpec() : null;
  }

  public static List<String> getLastProjection(Query query)
  {
    return query instanceof LastProjectionSupport ? ((LastProjectionSupport<?>) query).getOutputColumns() : null;
  }

  public static LateralViewSpec getLateralViewSpec(Query query)
  {
    return query instanceof LateralViewSupport ? ((LateralViewSupport) query).getLateralView() : null;
  }

  public static ViewDataSource asView(Query query, DimFilter filter, List<String> columns)
  {
    Preconditions.checkArgument(query.getDataSource() instanceof TableDataSource);
    return new ViewDataSource(
        ((TableDataSource) query.getDataSource()).getName(),
        columns,
        getVirtualColumns(query),
        filter,
        false
    );
  }

  @Override
  public Query<T> resolveQuery(Supplier<RowResolver> resolver, boolean expand)
  {
    Query<T> query = expand ? ColumnExpander.expand(this, resolver) : this;
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
    if (query instanceof Query.FilterSupport) {
      Query.FilterSupport<T> filterSupport = (Query.FilterSupport<T>) query;
      DimFilter filter = filterSupport.getFilter();
      if (filter != null) {
        DimFilter optimized = DimFilters.convertToCNF(filter).optimize();
        Map<String, String> aliasMapping = QueryUtils.aliasMapping(this);
        if (!aliasMapping.isEmpty()) {
          optimized = optimized.withRedirection(aliasMapping);
        }
        if (filter != optimized) {
          query = filterSupport.withFilter(optimized);
        }
      }
    }
    return query;
  }

  public static <T> Query<T> specialize(Query<T> query, Segment segment)
  {
    if (query instanceof Query.FilterSupport) {
      Query.FilterSupport<T> filterSupport = (Query.FilterSupport<T>) query;
      DimFilter filter = filterSupport.getFilter();
      if (filter != null) {
        DimFilter optimized = filter.specialize(segment, filterSupport.getVirtualColumns());
        if (filter != optimized) {
          query = filterSupport.withFilter(optimized);
        }
      }
    }
    return query;
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> responseContext)
  {
    return makeQueryRunner(walker).run(this, assertContext(responseContext));
  }

  private Map<String, Object> assertContext(Map<String, Object> context)
  {
    return context == null ? Maps.<String, Object>newHashMap() : context;
  }

  @Override
  public QueryRunner<T> makeQueryRunner(QuerySegmentWalker walker)
  {
    QuerySegmentSpec spec = querySegmentSpec == null ? QuerySegmentSpec.ETERNITY : querySegmentSpec;
    QueryRunner<T> runner = spec.lookup(this, walker);
    if (walker instanceof ForwardingSegmentWalker) {
      runner = ((ForwardingSegmentWalker) walker).handle(this, runner);
    }
    return runner;
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
      Duration totalDuration = Duration.ZERO;
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
  public boolean hasContext(String key)
  {
    return context != null && context.containsKey(key);
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
  public long getContextLong(String key, long defaultValue)
  {
    return PropUtils.parseLong(getContext(), key, defaultValue);
  }

  @Override
  public double getContextDouble(String key, double defaultValue)
  {
    return PropUtils.parseDouble(getContext(), key, defaultValue);
  }

  protected static final Map<String, Object> DEFAULT_DATALOCAL_CONTEXT = GuavaUtils.mutableMap(
      FINALIZE, false,
      BROKER_SIDE, false,
      POST_PROCESSING, null
  );

  @Override
  public Query<T> toLocalQuery()
  {
    return withOverriddenContext(DEFAULT_DATALOCAL_CONTEXT);
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

  public static Map<String, Object> copyContext(Query<?> query, String key, Object value)
  {
    Map<String, Object> context = copyContext(query);
    putTo(context, key, value);
    return context;
  }

  public static Map<String, Object> copyContextForMeta(Query<?> query)
  {
    return copyContextForMeta(query.getContext());
  }

  public static Map<String, Object> copyContextForMeta(Map<String, Object> context, String key, Object value)
  {
    final Map<String, Object> forMeta = copyContextForMeta(context);
    putTo(forMeta, key, value);
    return forMeta;
  }

  public static Map<String, Object> copyContextForMeta(Map<String, Object> context, String key1, Object value1, String key2, Object value2)
  {
    final Map<String, Object> forMeta = copyContextForMeta(context);
    putTo(forMeta, key1, value1);
    putTo(forMeta, key2, value2);
    return forMeta;
  }

  public static void putTo(Map<String, Object> context, String key, Object value)
  {
    if (value != null) {
      context.put(key, value);
    } else {
      context.remove(key);
    }
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
    Map<String, Object> remover = Maps.newHashMapWithExpectedSize(keys.length);
    for (String key : keys) {
      remover.put(key, null);
    }
    return remover;
  }

  protected Map<String, Object> computeOverriddenContext(Map<String, Object> overrides)
  {
    return overrideContextWith(getContext(), overrides);
  }

  protected Map<String, Object> computeOverriddenContext(String key, Object value)
  {
    return overrideContextWith(getContext(), value == null ? contextRemover(key) : ImmutableMap.of(key, value));
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
   * If "query" has a single universal timestamp, return it. Otherwise, return null. This is useful
   * for keeping timestamps in sync across partial queries that may have different intervals.
   *
   * @param query the query
   *
   * @return universal timestamp, or null
   */
  public static <T> Query<T> setUniversalTimestamp(Query<T> query)
  {
    final Number fudgeTimestamp = query.getContextValue(FUDGE_TIMESTAMP);
    if (fudgeTimestamp == null && Granularities.isAll(query.getGranularity())) {
      final List<Interval> intervals = query.getIntervals();
      if (!intervals.isEmpty()) {
        return query.withOverriddenContext(FUDGE_TIMESTAMP, intervals.get(0).getStartMillis());
      }
    }
    return query;
  }

  public static <T> Long getUniversalTimestamp(Query<T> query, Long defaultValue)
  {
    final Number fudgeTimestamp = query.getContextValue(FUDGE_TIMESTAMP);
    if (fudgeTimestamp != null) {
      return fudgeTimestamp.longValue();
    }
    return defaultValue;
  }

  @Override
  public Comparator<T> getMergeOrdering(List<String> columns)
  {
    final Comparator<T> retVal = GuavaUtils.<T>noNullableNatural();
    return descending ? Comparators.REVERT(retVal) : retVal;
  }

  @Override
  public String getId()
  {
    return getContextValue(QUERYID);
  }

  @Override
  public Query<T> withId(String id)
  {
    return withOverriddenContext(GuavaUtils.<String, Object>mutableMap(QUERYID, id));
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

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dataSource.hashCode();
    result = 31 * result + (descending ? 1 : 0);
    result = 31 * result + (querySegmentSpec != null ? querySegmentSpec.hashCode() : 0);
    return result;
  }

  protected KeyBuilder append(KeyBuilder builder)
  {
    return builder.append(DataSources.getName(getDataSource())).append(isDescending()).append(getQuerySegmentSpec());
  }
}
