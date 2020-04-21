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

package io.druid.query.select;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.LateralViewSpec;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryUtils;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.TableDataSource;
import io.druid.query.UnionDataSource;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.timeline.DataSegment;
import io.druid.timeline.DataSegmentUtils;
import io.druid.timeline.LogicalSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.ToIntFunction;

/**
 */
public class SelectQueryQueryToolChest extends QueryToolChest<Result<SelectResultValue>, SelectQuery>
{
  private static final TypeReference<Result<SelectResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<SelectResultValue>>()
      {
      };

  private final SelectQueryEngine engine;
  private final GenericQueryMetricsFactory metricsFactory;

  @Inject
  public SelectQueryQueryToolChest(
      SelectQueryEngine engine,
      GenericQueryMetricsFactory metricsFactory
  )
  {
    this.engine = engine;
    this.metricsFactory = metricsFactory;
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> mergeResults(
      QueryRunner<Result<SelectResultValue>> queryRunner
  )
  {
    return new ResultMergeQueryRunner<Result<SelectResultValue>>(queryRunner)
    {
      @Override
      public Sequence<Result<SelectResultValue>> doRun(
          QueryRunner<Result<SelectResultValue>> baseRunner,
          Query<Result<SelectResultValue>> query,
          Map<String, Object> context
      )
      {
        return super.doRun(baseRunner, query, context);
      }

      @Override
      protected Ordering<Result<SelectResultValue>> makeOrdering(Query<Result<SelectResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(query);
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
  public <I> QueryRunner<Result<SelectResultValue>> handleSubQuery(QuerySegmentWalker segmentWalker, QueryConfig config)
  {
    return new SubQueryRunner<I>(segmentWalker, config)
    {
      @Override
      public Sequence<Result<SelectResultValue>> run(
          Query<Result<SelectResultValue>> query,
          Map<String, Object> responseContext
      )
      {
        final SelectQuery outerQuery = (SelectQuery) query;
        if (outerQuery.getGranularity() != Granularities.ALL) {
          return super.run(query, responseContext);
        }
        final PagingSpec pagingSpec = outerQuery.getPagingSpec();
        if (!GuavaUtils.isNullOrEmpty(pagingSpec.getPagingIdentifiers())) {
          return super.run(query, responseContext);
        }
        final Query<I> innerQuery = ((QueryDataSource) outerQuery.getDataSource()).getQuery();
        if (!QueryUtils.coveredBy(innerQuery, outerQuery)) {
          return super.run(query, responseContext);
        }
        return runStreaming(query, responseContext);
      }

      @Override
      protected Function<Interval, Sequence<Result<SelectResultValue>>> query(
          final Query<Result<SelectResultValue>> query,
          final Segment segment
      )
      {
        final SelectQuery select = (SelectQuery) query;
        return new Function<Interval, Sequence<Result<SelectResultValue>>>()
        {
          @Override
          public Sequence<Result<SelectResultValue>> apply(Interval interval)
          {
            return engine.process(
                select.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)),
                config.getSelect(),
                segment
            );
          }
        };
      }

      @Override
      protected Function<Cursor, Sequence<Result<SelectResultValue>>> streamQuery(
          final Query<Result<SelectResultValue>> query
      )
      {
        return new Function<Cursor, Sequence<Result<SelectResultValue>>>()
        {
          @Override
          public Sequence<Result<SelectResultValue>> apply(Cursor cursor)
          {
            final SelectQuery select = (SelectQuery) query;
            final String concatString = select.getConcatString();
            final List<String> outputColumns = Lists.newArrayList();
            final List<ObjectColumnSelector> selectors = Lists.newArrayList();
            for (DimensionSpec dimensionSpec : select.getDimensions()) {
              DimensionSelector selector = cursor.makeDimensionSelector(dimensionSpec);
              selector = dimensionSpec.decorate(selector, cursor);
              if (concatString != null) {
                selectors.add(ColumnSelectors.asConcatValued(selector, concatString));
              } else {
                selectors.add(ColumnSelectors.asMultiValued(selector));
              }
              outputColumns.add(dimensionSpec.getOutputName());
            }
            for (String metric : select.getMetrics()) {
              selectors.add(cursor.makeObjectColumnSelector(metric));
              outputColumns.add(metric);
            }
            final Interval interval = JodaUtils.umbrellaInterval(select.getIntervals());
            final String segmentId = DataSegment.makeDataSegmentIdentifier(
                org.apache.commons.lang.StringUtils.join(select.getDataSource().getNames(), '_'),
                interval.getStart(),
                interval.getEnd(),
                "temporary",
                NoneShardSpec.instance()
            );

            final int limit = select.getPagingSpec().getThreshold();
            final List<EventHolder> events = Lists.newArrayList();
            for (; !cursor.isDone() && (limit < 0 || events.size() < limit); cursor.advance()) {
              Map<String, Object> event = Maps.newHashMap();
              for (int i = 0; i < selectors.size(); i++) {
                event.put(outputColumns.get(i), selectors.get(i).get());
              }
              events.add(new EventHolder(segmentId, events.size(), event));
            }

            SelectResultValue resultValue = new SelectResultValue(ImmutableMap.of(segmentId, events.size()), events);
            return Sequences.simple(
                Arrays.asList(
                    new Result<SelectResultValue>(interval.getStart(), resultValue)
                )
            );
          }
        };
      }
    };
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(SelectQuery query)
  {
    return metricsFactory.makeMetrics(query).granularity(query.getGranularity());
  }

  @Override
  public TypeReference<Result<SelectResultValue>> getResultTypeReference(SelectQuery query)
  {
    return TYPE_REFERENCE;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ToIntFunction numRows(SelectQuery query)
  {
    if (BaseQuery.isBySegment(query)) {
      return new ToIntFunction()
      {
        @Override
        public int applyAsInt(Object bySegment)
        {
          int counter = 0;
          for (Object value : BySegmentResultValueClass.unwrap(bySegment)) {
            counter += ((Result<SelectResultValue>) value).getValue().size();
          }
          return counter;
        }
      };
    }
    return new ToIntFunction()
    {
      @Override
      public int applyAsInt(Object value)
      {
        if (value instanceof Result) {
          return ((Result<SelectResultValue>) value).getValue().size();
        }
        return 1;
      }
    };
  }

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(SelectQuery query, List<T> segments)
  {
    return filterSegmentsOnPagingSpec(query, segments);
  }

  static <T extends LogicalSegment> List<T> filterSegmentsOnPagingSpec(SelectQuery query, List<T> segments)
  {
    // at the point where this code is called, only one datasource should exist.
    String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());

    PagingSpec pagingSpec = query.getPagingSpec();
    Map<String, Integer> paging = pagingSpec.getPagingIdentifiers();
    if (paging == null || paging.isEmpty()) {
      return segments;
    }

    final Granularity granularity = query.getGranularity();

    List<Interval> intervals = Lists.newArrayList(
        Iterables.transform(paging.keySet(), DataSegmentUtils.INTERVAL_EXTRACTOR(dataSource))
    );
    Collections.sort(
        intervals, query.isDescending() ? JodaUtils.intervalsByEndThenStart()
                                        : JodaUtils.intervalsByStartThenEnd()
    );

    TreeMap<Long, Long> granularThresholds = Maps.newTreeMap();
    for (Interval interval : intervals) {
      if (query.isDescending()) {
        long granularEnd = granularity.bucketStart(interval.getEnd()).getMillis();
        Long currentEnd = granularThresholds.get(granularEnd);
        if (currentEnd == null || interval.getEndMillis() > currentEnd) {
          granularThresholds.put(granularEnd, interval.getEndMillis());
        }
      } else {
        long granularStart = granularity.bucketStart(interval.getStart()).getMillis();
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
        Map.Entry<Long, Long> ceiling = granularThresholds.ceilingEntry(granularity.bucketStart(interval.getEnd()).getMillis());
        if (ceiling == null || interval.getStartMillis() >= ceiling.getValue()) {
          it.remove();
        }
      }
    } else {
      while (it.hasNext()) {
        Interval interval = it.next().getInterval();
        Map.Entry<Long, Long> floor = granularThresholds.floorEntry(granularity.bucketStart(interval.getStart()).getMillis());
        if (floor == null || interval.getEndMillis() <= floor.getValue()) {
          it.remove();
        }
      }
    }
    return queryIntervals;
  }

  @Override
  public SelectQuery optimizeQuery(SelectQuery query, QuerySegmentWalker walker)
  {
    PagingSpec pagingSpec = query.getPagingSpec();
    if (!pagingSpec.getPagingIdentifiers().isEmpty() || pagingSpec.getThreshold() == 0) {
      // todo
      return query;
    }

    int threshold = pagingSpec.getThreshold();
    List<String> dataSourceNames = query.getDataSource().getNames();

    SelectMetaQuery metaQuery = query.toMetaQuery(false);
    List<Result<SelectMetaResultValue>> results = Sequences.toList(QueryRunners.run(metaQuery, walker));

    Comparator<Interval> comparator = query.isDescending() ? JodaUtils.intervalsByEndThenStart()
                                                           : JodaUtils.intervalsByStartThenEnd();
    int totalSegments = 0;
    Map<String, Map<Interval, MutableInt>> mapping = Maps.newHashMap();
    for (Result<SelectMetaResultValue> result : results) {
      Map<String, Integer> perSegmentCounts = new TreeMap<>(result.getValue().getPerSegmentCounts());
      for (Map.Entry<String, Integer> entry : perSegmentCounts.entrySet()) {
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
      totalSegments += perSegmentCounts.size();
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
      LOG.info("Filtered %d segment interval into %d", totalSegments, targetIntervals.size());
      return query.withDataSource(new TableDataSource(targetDataSources.get(0)))
                  .withQuerySegmentSpec(new MultipleIntervalSegmentSpec(targetIntervals));
    }
    LOG.info("Filtered %d dataSource into %d", mapping.size(), targetDataSources.size());
    return query.withDataSource(new UnionDataSource(targetDataSources));
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
  public Function<Sequence<Result<SelectResultValue>>, Sequence<Map<String, Object>>> asMap(
      final SelectQuery query,
      final String timestampColumn
  )
  {
    return new Function<Sequence<Result<SelectResultValue>>, Sequence<Map<String,Object>>>()
    {
      @Override
      public Sequence<Map<String, Object>> apply(Sequence<Result<SelectResultValue>> sequence)
      {
        return Sequences.explode(
            sequence, new Function<Result<SelectResultValue>, Sequence<Map<String, Object>>>()
            {
              @Override
              public Sequence<Map<String, Object>> apply(Result<SelectResultValue> input)
              {
                return Sequences.simple(
                    Iterables.transform(
                        input.getValue().getEvents(), new Function<EventHolder, Map<String, Object>>()
                        {
                          @Override
                          public Map<String, Object> apply(EventHolder input)
                          {
                            Map<String, Object> event = input.getEvent();
                            if (timestampColumn != null) {
                              if (!MapBasedRow.supportInplaceUpdate(event)) {
                                event = Maps.newLinkedHashMap(event);
                              }
                              event.put(timestampColumn, input.getTimestamp());
                            }
                            return event;
                          }
                        }
                    )
                );
              }
            }
        );
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
        final Sequence<Result<SelectResultValue>> result;
        final List<String> outputColumns = ((SelectQuery) query).getOutputColumns();
        final LateralViewSpec lateralViewSpec = ((SelectQuery) query).getLateralView();
        if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
          result = Sequences.map(
              runner.run(query, responseContext),
              new Function<Result<SelectResultValue>, Result<SelectResultValue>>()
              {
                @Override
                public Result<SelectResultValue> apply(Result<SelectResultValue> input)
                {
                  final DateTime timestamp = input.getTimestamp();
                  final SelectResultValue value = input.getValue();
                  final List<EventHolder> events = value.getEvents();
                  final List<EventHolder> processed = Lists.newArrayListWithExpectedSize(events.size());
                  for (EventHolder holder : events) {
                    Map<String, Object> original = holder.getEvent();
                    Map<String, Object> retained = Maps.newHashMapWithExpectedSize(outputColumns.size());
                    for (String retain : outputColumns) {
                      retained.put(retain, original.get(retain));
                    }
                    processed.add(new EventHolder(holder.getSegmentId(), holder.getOffset(), retained));
                  }
                  return new Result<>(timestamp, new SelectResultValue(value.getPagingIdentifiers(), processed));
                }
              }
          );
        } else {
          result = runner.run(query, responseContext);
        }
        return lateralViewSpec != null ? toLateralView(result, lateralViewSpec) : result;
      }
    };
  }

  Sequence<Result<SelectResultValue>> toLateralView(
      Sequence<Result<SelectResultValue>> result, final LateralViewSpec lateralViewSpec
  )
  {
    final Function<Map<String, Object>, Iterable<Map<String, Object>>> function = lateralViewSpec.prepare();
    return Sequences.map(
        result, new Function<Result<SelectResultValue>, Result<SelectResultValue>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Result<SelectResultValue> apply(Result<SelectResultValue> input)
          {
            final DateTime timestamp = input.getTimestamp();
            final SelectResultValue value = input.getValue();

            Iterable<EventHolder> transform = Iterables.concat(
                Iterables.transform(
                    value.getEvents(), new Function<EventHolder, Iterable<EventHolder>>()
                    {
                      @Override
                      public Iterable<EventHolder> apply(final EventHolder input)
                      {
                        final String segmentId = input.getSegmentId();
                        final int offset = input.getOffset();
                        return Iterables.transform(
                            function.apply(input.getEvent()),
                            new Function<Map<String, Object>, EventHolder>()
                            {
                              @Override
                              public EventHolder apply(Map<String, Object> event)
                              {
                                return new EventHolder(segmentId, offset, event);
                              }
                            }
                        );
                      }
                    }
                )
            );
            return new Result(
                timestamp,
                new SelectResultValue(value.getPagingIdentifiers(), Lists.newArrayList(transform))
            );
          }
        }
    );
  }
}
