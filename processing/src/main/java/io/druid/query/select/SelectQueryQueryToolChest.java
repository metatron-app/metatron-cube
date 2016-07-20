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

package io.druid.query.select;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.StringUtils;
import com.metamx.common.guava.Comparators;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.granularity.QueryGranularity;
import io.druid.query.CacheStrategy;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.TableDataSource;
import io.druid.query.TabularFormat;
import io.druid.query.UnionDataSource;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.VirtualColumn;
import io.druid.timeline.DataSegmentUtils;
import io.druid.timeline.LogicalSegment;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 */
public class SelectQueryQueryToolChest extends QueryToolChest<Result<SelectResultValue>, SelectQuery>
{
  private static final byte SELECT_QUERY = 0x13;
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<Result<SelectResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<SelectResultValue>>()
      {
      };

  private final ObjectMapper jsonMapper;

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;
  private final QuerySegmentWalker segmentWalker;

  @Inject
  public SelectQueryQueryToolChest(
      ObjectMapper jsonMapper,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator,
      QuerySegmentWalker segmentWalker
  )
  {
    this.jsonMapper = jsonMapper;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
    this.segmentWalker = segmentWalker;
  }

  public SelectQueryQueryToolChest(
      ObjectMapper jsonMapper,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this(jsonMapper, intervalChunkingQueryRunnerDecorator, null);
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> mergeResults(
      QueryRunner<Result<SelectResultValue>> queryRunner
  )
  {
    return new ResultMergeQueryRunner<Result<SelectResultValue>>(queryRunner)
    {
      @Override
      protected Ordering<Result<SelectResultValue>> makeOrdering(Query<Result<SelectResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(
            ((SelectQuery) query).getGranularity(), query.isDescending()
        );
      }

      @Override
      protected BinaryFn<Result<SelectResultValue>, Result<SelectResultValue>, Result<SelectResultValue>> createMergeFn(
          Query<Result<SelectResultValue>> input
      )
      {
        SelectQuery query = (SelectQuery) input;
        return new SelectBinaryFn(
            query.getGranularity(),
            query.getPagingSpec(),
            query.isDescending()
        );
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SelectQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<Result<SelectResultValue>, Result<SelectResultValue>> makePreComputeManipulatorFn(
      final SelectQuery query, final MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<SelectResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<SelectResultValue>, Object, SelectQuery> getCacheStrategy(final SelectQuery query)
  {
    return new CacheStrategy<Result<SelectResultValue>, Object, SelectQuery>()
    {
      @Override
      public byte[] computeCacheKey(SelectQuery query)
      {
        final DimFilter dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] granularityBytes = query.getGranularity().cacheKey();

        List<DimensionSpec> dimensionSpecs = query.getDimensions();
        if (dimensionSpecs == null) {
          dimensionSpecs = Collections.emptyList();
        }

        final byte[][] dimensionsBytes = new byte[dimensionSpecs.size()][];
        int dimensionsBytesSize = 0;
        int index = 0;
        for (DimensionSpec dimension : dimensionSpecs) {
          dimensionsBytes[index] = dimension.getCacheKey();
          dimensionsBytesSize += dimensionsBytes[index].length;
          ++index;
        }

        final Set<String> metrics = Sets.newTreeSet();
        if (query.getMetrics() != null) {
          metrics.addAll(query.getMetrics());
        }

        final byte[][] metricBytes = new byte[metrics.size()][];
        int metricBytesSize = 0;
        index = 0;
        for (String metric : metrics) {
          metricBytes[index] = StringUtils.toUtf8(metric);
          metricBytesSize += metricBytes[index].length;
          ++index;
        }

        List<VirtualColumn> virtualColumns = query.getVirtualColumns();
        if (virtualColumns == null) {
          virtualColumns = Collections.emptyList();
        }

        final byte[][] virtualColumnsBytes = new byte[virtualColumns.size()][];
        int virtualColumnsBytesSize = 0;
        index = 0;
        for (VirtualColumn vc : virtualColumns) {
          virtualColumnsBytes[index] = vc.getCacheKey();
          virtualColumnsBytesSize += virtualColumnsBytes[index].length;
          ++index;
        }
        final byte[] outputColumnsBytes = QueryCacheHelper.computeCacheBytes(query.getOutputColumns());

        final ByteBuffer queryCacheKey = ByteBuffer
            .allocate(
                1
                + granularityBytes.length
                + filterBytes.length
                + query.getPagingSpec().getCacheKey().length
                + dimensionsBytesSize
                + metricBytesSize
                + virtualColumnsBytesSize
                + outputColumnsBytes.length
            )
            .put(SELECT_QUERY)
            .put(granularityBytes)
            .put(filterBytes)
            .put(query.getPagingSpec().getCacheKey())
            .put(outputColumnsBytes);

        for (byte[] dimensionsByte : dimensionsBytes) {
          queryCacheKey.put(dimensionsByte);
        }

        for (byte[] metricByte : metricBytes) {
          queryCacheKey.put(metricByte);
        }

        for (byte[] vcByte : virtualColumnsBytes) {
          queryCacheKey.put(vcByte);
        }

        return queryCacheKey.array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<SelectResultValue>, Object> prepareForCache()
      {
        return new Function<Result<SelectResultValue>, Object>()
        {
          @Override
          public Object apply(final Result<SelectResultValue> input)
          {
            return Arrays.asList(
                input.getTimestamp().getMillis(),
                input.getValue().getPagingIdentifiers(),
                input.getValue().getEvents()
            );
          }
        };
      }

      @Override
      public Function<Object, Result<SelectResultValue>> pullFromCache()
      {
        return new Function<Object, Result<SelectResultValue>>()
        {
          private final QueryGranularity granularity = query.getGranularity();

          @Override
          public Result<SelectResultValue> apply(Object input)
          {
            List<Object> results = (List<Object>) input;
            Iterator<Object> resultIter = results.iterator();

            DateTime timestamp = granularity.toDateTime(((Number) resultIter.next()).longValue());

            return new Result<SelectResultValue>(
                timestamp,
                new SelectResultValue(
                    (Map<String, Integer>) jsonMapper.convertValue(
                        resultIter.next(), new TypeReference<Map<String, Integer>>()
                        {
                        }
                    ),
                    (List<EventHolder>) jsonMapper.convertValue(
                        resultIter.next(), new TypeReference<List<EventHolder>>()
                        {
                        }
                    )
                )
            );
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> preMergeQueryDecoration(final QueryRunner<Result<SelectResultValue>> runner)
  {
    return intervalChunkingQueryRunnerDecorator.decorate(
        new QueryRunner<Result<SelectResultValue>>()
        {
          @Override
          public Sequence<Result<SelectResultValue>> run(
              Query<Result<SelectResultValue>> query, Map<String, Object> responseContext
          )
          {
            SelectQuery selectQuery = (SelectQuery) query;
            if (selectQuery.getDimensionsFilter() != null) {
              selectQuery = selectQuery.withDimFilter(selectQuery.getDimensionsFilter().optimize());
            }
            return runner.run(selectQuery, responseContext);
          }
        }, this
    );
  }

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(SelectQuery query, List<T> segments)
  {
    // at the point where this code is called, only one datasource should exist.
    String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());

    PagingSpec pagingSpec = query.getPagingSpec();
    Map<String, Integer> paging = pagingSpec.getPagingIdentifiers();
    if (paging == null || paging.isEmpty()) {
      return segments;
    }

    final QueryGranularity granularity = query.getGranularity();

    List<Interval> intervals = Lists.newArrayList(
        Iterables.transform(paging.keySet(), DataSegmentUtils.INTERVAL_EXTRACTOR(dataSource))
    );
    Collections.sort(
        intervals, query.isDescending() ? Comparators.intervalsByEndThenStart()
                                        : Comparators.intervalsByStartThenEnd()
    );

    TreeMap<Long, Long> granularThresholds = Maps.newTreeMap();
    for (Interval interval : intervals) {
      if (query.isDescending()) {
        long granularEnd = granularity.truncate(interval.getEndMillis());
        Long currentEnd = granularThresholds.get(granularEnd);
        if (currentEnd == null || interval.getEndMillis() > currentEnd) {
          granularThresholds.put(granularEnd, interval.getEndMillis());
        }
      } else {
        long granularStart = granularity.truncate(interval.getStartMillis());
        Long currentStart = granularThresholds.get(granularStart);
        if (currentStart == null || interval.getStartMillis() < currentStart) {
          granularThresholds.put(granularStart, interval.getStartMillis());
        }
      }
    }

    List<T> queryIntervals = Lists.newArrayList(segments);

    Iterator<T> it = queryIntervals.iterator();
    if (query.isDescending()) {
      while (it.hasNext()) {
        Interval interval = it.next().getInterval();
        Map.Entry<Long, Long> ceiling = granularThresholds.ceilingEntry(granularity.truncate(interval.getEndMillis()));
        if (ceiling == null || interval.getStartMillis() >= ceiling.getValue()) {
          it.remove();
        }
      }
    } else {
      while (it.hasNext()) {
        Interval interval = it.next().getInterval();
        Map.Entry<Long, Long> floor = granularThresholds.floorEntry(granularity.truncate(interval.getStartMillis()));
        if (floor == null || interval.getEndMillis() <= floor.getValue()) {
          it.remove();
        }
      }
    }
    return queryIntervals;
  }

  @Override
  public SelectQuery rewriteQuery(SelectQuery query, QuerySegmentWalker walker)
  {
    if (!query.getPagingSpec().getPagingIdentifiers().isEmpty()) {
      // todo
      return query;
    }

    int threshold = query.getPagingSpec().getThreshold();
    List<String> dataSourceNames = query.getDataSource().getNames();
    Map<String, Object> context = Maps.newHashMap();
    List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    SelectMetaQuery metaQuery = new SelectMetaQuery(
        query.getDataSource(), query.getQuerySegmentSpec(),
        query.getDimensionsFilter(), query.getGranularity(), context
    );
    List<Result<SelectMetaResultValue>> results =
        Sequences.toList(
            segmentWalker.getQueryRunnerForIntervals(metaQuery, intervals).run(metaQuery, context),
            Lists.<Result<SelectMetaResultValue>>newArrayList()
        );

    Comparator<Interval> comparator = query.isDescending() ? Comparators.intervalsByEndThenStart()
                                                           : Comparators.intervalsByStartThenEnd();
    Map<String, Map<Interval, MutableInt>> mapping = Maps.newHashMap();
    for (Result<SelectMetaResultValue> result : results) {
      for (Map.Entry<String, Integer> entry : result.getValue().getPerSegmentCounts().entrySet()) {
        String identifier = entry.getKey();
        MutableInt value = new MutableInt(entry.getValue());
        String dataSource = findDataSource(dataSourceNames, identifier);
        Map<Interval, MutableInt> counts = mapping.get(dataSource);
        if (counts == null) {
          mapping.put(dataSource, counts = Maps.newTreeMap(comparator));
        }
        Interval interval = DataSegmentUtils.INTERVAL_EXTRACTOR(dataSource).apply(identifier);
        MutableInt prev = counts.put(interval, value);
        if (prev != null) {
          value.add(prev.intValue());
        }
      }
    }
    List<String> targetDataSources = Lists.newArrayList();
    List<Interval> targetIntervals = Lists.newArrayList();
    for (String dataSource : dataSourceNames) {
      Map<Interval, MutableInt> counts = mapping.get(dataSource);
      if (counts == null) {
        continue;
      }
      targetDataSources.add(dataSource);
      boolean singleDatasource = targetDataSources.size() == 1;
      for (Map.Entry<Interval, MutableInt> entry : counts.entrySet()) {
        if (singleDatasource) {
          targetIntervals.add(entry.getKey());
        }
        threshold -= entry.getValue().intValue();
        if (singleDatasource && threshold < 0) {
          break;
        }
      }
      if (threshold < 0) {
        break;
      }
    }
    if (targetDataSources.size() == 1) {
      return query.withDataSource(new TableDataSource(targetDataSources.get(0)))
                  .withQuerySegmentSpec(new MultipleIntervalSegmentSpec(targetIntervals));
    }
    return query.withDataSource(new UnionDataSource(TableDataSource.of(targetDataSources)));
  }

  private String findDataSource(List<String> dataSourceNames, String identifier)
  {
    String candidate = null;
    for (String dataSourceName : dataSourceNames) {
      if (identifier.startsWith(dataSourceName + "_")) {
        if (candidate == null || candidate.length() < dataSourceName.length()) {
          candidate = dataSourceName;
        }
      }
    }
    if (candidate == null) {
      throw new IllegalStateException("invalid identifier " + identifier);
    }
    return candidate;
  }

  @Override
  public TabularFormat toTabularFormat(final Sequence<Result<SelectResultValue>> sequence)
  {
    return new TabularFormat()
    {
      final Map<String, Object> pagingSpec = Maps.newHashMap();

      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return Sequences.concat(
            Sequences.map(
                sequence, new Function<Result<SelectResultValue>, Sequence<Map<String, Object>>>()
                {
                  @Override
                  public Sequence<Map<String, Object>> apply(Result<SelectResultValue> input)
                  {
                    pagingSpec.putAll(input.getValue().getPagingIdentifiers());
                    return Sequences.simple(
                        Iterables.transform(
                            input.getValue().getEvents(), new Function<EventHolder, Map<String, Object>>()
                            {
                              @Override
                              public Map<String, Object> apply(EventHolder input)
                              {
                                return input.getEvent();
                              }
                            }
                        )
                    );
                  }
                }
            )
        );
      }

      @Override
      public Map<String, Object> getMetaData()
      {
        return pagingSpec;
      }
    };
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> finalQueryDecoration(final QueryRunner<Result<SelectResultValue>> runner)
  {
    return new QueryRunner<Result<SelectResultValue>>()
    {
      @Override
      public Sequence<Result<SelectResultValue>> run(
          Query<Result<SelectResultValue>> query, Map<String, Object> responseContext
      )
      {
        final List<String> outputColumns = ((SelectQuery)query).getOutputColumns();
        final Sequence<Result<SelectResultValue>> result = runner.run(query, responseContext);
        if (outputColumns != null) {
          return Sequences.map(
              result, new Function<Result<SelectResultValue>, Result<SelectResultValue>>()
              {
                @Override
                public Result<SelectResultValue> apply(Result<SelectResultValue> input)
                {
                  DateTime timestamp = input.getTimestamp();
                  SelectResultValue value = input.getValue();
                  List<EventHolder> processed = Lists.newArrayListWithExpectedSize(value.getEvents().size());
                  for (EventHolder holder : value.getEvents()) {
                    Map<String, Object> original = holder.getEvent();
                    Map<String, Object> retained = Maps.newHashMapWithExpectedSize(outputColumns.size());
                    for (String retain : outputColumns) {
                      retained.put(retain, original.get(retain));
                    }
                    processed.add(new EventHolder(holder.getSegmentId(), holder.getOffset(), retained));
                  }
                  return new Result(timestamp, new SelectResultValue(value.getPagingIdentifiers(), processed));
                }
              }
          );
        } else {
          return result;
        }
      }
    };
  }
}
