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
import com.metamx.common.StringUtils;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.ViewSupportHelper;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import org.joda.time.Duration;
import org.joda.time.Interval;

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

  public static <T> boolean getContextBySegment(Query<T> query, boolean defaultValue)
  {
    return PropUtils.parseBoolean(query.getContext(), BY_SEGMENT, defaultValue);
  }

  public static <T> boolean getContextPopulateCache(Query<T> query, boolean defaultValue)
  {
    return PropUtils.parseBoolean(query.getContext(), "populateCache", defaultValue);
  }

  public static <T> boolean getContextUseCache(Query<T> query, boolean defaultValue)
  {
    return PropUtils.parseBoolean(query.getContext(), "useCache", defaultValue);
  }

  public static <T> boolean getContextFinalize(Query<T> query, boolean defaultValue)
  {
    return PropUtils.parseBoolean(query.getContext(), FINALIZE, defaultValue);
  }

  public static <T> boolean optimizeQuery(Query<T> query, boolean defaultValue)
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
  @Override
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return querySegmentSpec;
  }

  @Override
  public boolean hasFilters()
  {
    if (this instanceof DimFilterSupport) {
      if (((DimFilterSupport)this).getDimFilter() != null) {
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

  public static VirtualColumns getVirtualColumns(Query query)
  {
    List<VirtualColumn> vcs = Lists.newArrayList();
    if (query instanceof VCSupport) {
      vcs.addAll(((VCSupport<?>) query).getVirtualColumns());
    }
    if (query.getDataSource() instanceof ViewDataSource) {
      vcs.addAll(((ViewDataSource)query.getDataSource()).getVirtualColumns());
    }
    return VirtualColumns.valueOf(vcs);
  }

  public static DimFilter getDimFilter(Query query)
  {
    return query instanceof DimFilterSupport ? ((DimFilterSupport)query).getDimFilter() : null;
  }

  @Override
  public Query<T> resolveQuery(Supplier<RowResolver> resolver, ObjectMapper mapper)
  {
    Query<T> query = ViewSupportHelper.rewrite(this, resolver);
    if (query instanceof AggregationsSupport) {
      @SuppressWarnings("unchecked")
      AggregationsSupport<T> aggregationsSupport = (AggregationsSupport) query;
      boolean changed = false;
      List<AggregatorFactory> resolved = Lists.newArrayList();
      for (AggregatorFactory factory : aggregationsSupport.getAggregatorSpecs()) {
        if (!(factory instanceof AggregatorFactory.TypeResolving)) {
          resolved.add(factory);
          continue;
        }
        AggregatorFactory.TypeResolving resolving = (AggregatorFactory.TypeResolving) factory;
        if (resolving.needResolving()) {
          factory = resolving.resolve(resolver, mapper);
          changed = true;
        }
        resolved.add(factory);
      }
      if (changed) {
        query = aggregationsSupport.withAggregatorSpecs(resolved);
      }
    }
    return query;
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context)
  {
    return querySegmentSpec == null ? Sequences.<T>empty() : run(querySegmentSpec.lookup(this, walker), context);
  }

  @Override
  public Sequence<T> run(QueryRunner<T> runner, Map<String, Object> context)
  {
    return runner.run(this, context);
  }

  @Override
  public List<Interval> getIntervals()
  {
    return querySegmentSpec == null ? ImmutableList.<Interval>of() : querySegmentSpec.getIntervals();
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
    return withOverriddenContext(
        ImmutableMap.of(Preconditions.checkNotNull(contextKey), Preconditions.checkNotNull(contextValue))
    );
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

  public static Map<String, Object> removeContext(String... keys)
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
      if (override.getValue() == null) {
        overridden.remove(override.getKey());
      } else {
        overridden.put(override.getKey(), override.getValue());
      }
    }

    return overridden;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Ordering<T> getResultOrdering()
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
    if (!Objects.equals(context, baseQuery.context)) {
      return false;
    }
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
    result = 31 * result + (context != null ? context.hashCode() : 0);
    result = 31 * result + (querySegmentSpec != null ? querySegmentSpec.hashCode() : 0);
    result = 31 * result + (duration != null ? duration.hashCode() : 0);
    return result;
  }
}
