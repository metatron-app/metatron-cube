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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.StringUtils;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.PropUtils;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
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
  private static final QuerySegmentSpec EMPTY = new MultipleIntervalSegmentSpec(Arrays.<Interval>asList());

  @SuppressWarnings("unchecked")
  public static Query getRepresentative(Query query)
  {
    Query represent = query instanceof UnionAllQuery ? ((UnionAllQuery) query).getRepresentative() : query;
    if (represent != query) {
      return getRepresentative(represent);
    }
    return represent;
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

  public static final String QUERYID = "queryId";
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
    Preconditions.checkNotNull(dataSource, "dataSource can't be null");
    Preconditions.checkNotNull(dataSource, "dataSource can't be null");

    this.dataSource = dataSource;
    this.querySegmentSpec = querySegmentSpec == null ? EMPTY : querySegmentSpec;
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
    return this instanceof DimFilterSupport && DataSources.hasFilter(getDataSource());
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

  public boolean needsSchemaResolution()
  {
    return needsSchemaResolution(this);
  }

  public static boolean needsSchemaResolution(Query query)
  {
    if (!(query instanceof DimFilterSupport)) {
      return false;
    }
    if (query instanceof DimensionSupport) {
      DimensionSupport dimSupport = (DimensionSupport) query;
      if (dimSupport.getDimensions().isEmpty() && dimSupport.allDimensionsForEmpty()) {
        return true;
      }
    }
    if (query instanceof AggregationsSupport) {
      AggregationsSupport aggrSupport = (AggregationsSupport) query;
      if (aggrSupport.getAggregatorSpecs().isEmpty() && aggrSupport.allMetricsForEmpty()) {
        return true;
      }
    }
    if (query instanceof MetricSupport) {
      MetricSupport metricSupport = (MetricSupport) query;
      if (metricSupport.getMetrics().isEmpty() && metricSupport.allMetricsForEmpty()) {
        return true;
      }
    }
    if (query.getDataSource() instanceof ViewDataSource) {
          ViewDataSource view = (ViewDataSource) query.getDataSource();
      return !view.getColumns().isEmpty();
    }
    return false;
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context)
  {
    return run(querySegmentSpec.lookup(this, walker), context);
  }

  public Sequence<T> run(QueryRunner<T> runner, Map<String, Object> context)
  {
    return runner.run(this, context);
  }

  @Override
  public List<Interval> getIntervals()
  {
    return querySegmentSpec.getIntervals();
  }

  @Override
  public Duration getDuration()
  {
    if (duration == null) {
      Duration totalDuration = new Duration(0);
      for (Interval interval : querySegmentSpec.getIntervals()) {
        if (interval != null) {
          totalDuration = totalDuration.plus(interval.toDuration());
        }
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

  public Query<T> withOverriddenContext(String key, Object value)
  {
    return withOverriddenContext(ImmutableMap.of(Preconditions.checkNotNull(key), Preconditions.checkNotNull(value)));
  }

  public static Map<String, Object> removeContext(String... keys)
  {
    Map<String, Object> remover = Maps.newHashMap();
    for (String key : keys) {
      remover.put(key, null);
    }
    return remover;
  }

  protected Map<String, Object> computeOverridenContext(Map<String, Object> overrides)
  {
    return overrideContextWith(getContext(), overrides);
  }

  static Map<String, Object> overrideContextWith(Map<String, Object> context, Map<String, Object> overrides)
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
  public Ordering<T> getResultOrdering()
  {
    Ordering retVal = Ordering.natural();
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

    if (!dataSource.equals(baseQuery.dataSource)) {
      return false;
    }
    if (descending != baseQuery.descending) {
      return false;
    }
    if (context != null ? !context.equals(baseQuery.context) : baseQuery.context != null) {
      return false;
    }
    if (querySegmentSpec != null
        ? !querySegmentSpec.equals(baseQuery.querySegmentSpec)
        : baseQuery.querySegmentSpec != null) {
      return false;
    }
    if (duration != null ? !duration.equals(baseQuery.duration) : baseQuery.duration != null) {
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
