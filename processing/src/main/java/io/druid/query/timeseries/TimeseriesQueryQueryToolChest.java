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

package io.druid.query.timeseries;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.common.guava.GuavaUtils;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.CacheStrategy;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.LateralViewSpec;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class TimeseriesQueryQueryToolChest extends QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery>
{
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<Result<TimeseriesResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<TimeseriesResultValue>>()
      {
      };

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public TimeseriesQueryQueryToolChest(IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator)
  {
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> mergeResults(
      QueryRunner<Result<TimeseriesResultValue>> queryRunner
  )
  {
    return new ResultMergeQueryRunner<Result<TimeseriesResultValue>>(queryRunner)
    {
      @Override
      public Sequence<Result<TimeseriesResultValue>> doRun(QueryRunner<Result<TimeseriesResultValue>> baseRunner, Query<Result<TimeseriesResultValue>> query, Map<String, Object> context)
      {
        if (query.getContextBoolean(QueryContextKeys.FINAL_WORK, true)) {
          TimeseriesQuery timeseriesQuery = (TimeseriesQuery) query;
          query = timeseriesQuery.withPostAggregatorSpecs(null)
                                 .withLimitSpec(null)
                                 .withHavingSpec(null)
                                 .withOutputColumns(null)
                                 .withLateralView(null)
                                 .withOverriddenContext(QueryContextKeys.FINAL_WORK, false)
                                 .withOverriddenContext(BaseQuery.removeContext(QueryContextKeys.POST_PROCESSING));
        }
        return super.doRun(baseRunner, query, context);
      }

      @Override
      protected Ordering<Result<TimeseriesResultValue>> makeOrdering(Query<Result<TimeseriesResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(
            ((TimeseriesQuery) query).getGranularity(), query.isDescending()
        );
      }

      @Override
      protected BinaryFn<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> createMergeFn(
          Query<Result<TimeseriesResultValue>> input
      )
      {
        TimeseriesQuery query = (TimeseriesQuery) input;
        return new TimeseriesBinaryFn(
            query.getGranularity(),
            query.getAggregatorSpecs()
        );
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(TimeseriesQuery query)
  {
    final List<AggregatorFactory> aggregators = query.getAggregatorSpecs();
    return super.makeMetricBuilder(query)
                .setDimension("numMetrics", String.valueOf(aggregators.size()))
                .setDimension("numComplexMetrics", String.valueOf(DruidMetrics.findNumComplexAggs(aggregators)));
  }

  @Override
  public TypeReference<Result<TimeseriesResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<TimeseriesResultValue>, Object, TimeseriesQuery> getCacheStrategy(final TimeseriesQuery query)
  {
    return new CacheStrategy<Result<TimeseriesResultValue>, Object, TimeseriesQuery>()
    {
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();

      @Override
      public byte[] computeCacheKey(TimeseriesQuery query)
      {
        final DimFilter dimFilter = query.getDimFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(query.getAggregatorSpecs());
        final byte[] granularityBytes = query.getGranularity().getCacheKey();
        final byte descending = query.isDescending() ? (byte) 1 : 0;
        final byte skipEmptyBuckets = query.isSkipEmptyBuckets() ? (byte) 1 : 0;

        return ByteBuffer
            .allocate(
                3
                + granularityBytes.length
                + filterBytes.length
                + aggregatorBytes.length
            )
            .put(TIMESERIES_QUERY)
            .put(descending)
            .put(skipEmptyBuckets)
            .put(granularityBytes)
            .put(filterBytes)
            .put(aggregatorBytes)
            .array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TimeseriesResultValue>, Object> prepareForCache()
      {
        return new Function<Result<TimeseriesResultValue>, Object>()
        {
          @Override
          public Object apply(final Result<TimeseriesResultValue> input)
          {
            TimeseriesResultValue results = input.getValue();
            final List<Object> retVal = Lists.newArrayListWithCapacity(1 + aggs.size());

            retVal.add(input.getTimestamp().getMillis());
            for (AggregatorFactory agg : aggs) {
              retVal.add(results.getMetric(agg.getName()));
            }

            return retVal;
          }
        };
      }

      @Override
      public Function<Object, Result<TimeseriesResultValue>> pullFromCache()
      {
        return new Function<Object, Result<TimeseriesResultValue>>()
        {
          private final Granularity granularity = query.getGranularity();

          @Override
          public Result<TimeseriesResultValue> apply(Object input)
          {
            List<Object> results = (List<Object>) input;
            Map<String, Object> retVal = Maps.newLinkedHashMap();

            Iterator<AggregatorFactory> aggsIter = aggs.iterator();
            Iterator<Object> resultIter = results.iterator();

            DateTime timestamp = granularity.toDateTime(((Number) resultIter.next()).longValue());

            while (aggsIter.hasNext() && resultIter.hasNext()) {
              final AggregatorFactory factory = aggsIter.next();
              retVal.put(factory.getName(), factory.deserialize(resultIter.next()));
            }

            return new Result<TimeseriesResultValue>(
                timestamp,
                new TimeseriesResultValue(retVal)
            );
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> preMergeQueryDecoration(final QueryRunner<Result<TimeseriesResultValue>> runner)
  {
    return intervalChunkingQueryRunnerDecorator.decorate(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              Query<Result<TimeseriesResultValue>> query, Map<String, Object> responseContext
          )
          {
            TimeseriesQuery timeseriesQuery = (TimeseriesQuery) query;
            if (timeseriesQuery.getDimFilter() != null) {
              timeseriesQuery = timeseriesQuery.withDimFilter(timeseriesQuery.getDimFilter().optimize());
            }
            return runner.run(timeseriesQuery, responseContext);
          }
        }, this
    );
  }

  @Override
  public Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makePreComputeManipulatorFn(
      final TimeseriesQuery query, final MetricManipulationFn fn
  )
  {
    return makeComputeManipulatorFn(query, fn, false);
  }

  @Override
  public Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makePostComputeManipulatorFn(
      TimeseriesQuery query, MetricManipulationFn fn
  )
  {
    return makeComputeManipulatorFn(query, fn, true);
  }

  private Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makeComputeManipulatorFn(
      final TimeseriesQuery query, final MetricManipulationFn fn, final boolean calculatePostAggs
  )
  {
    final List<PostAggregator> postAggregators = PostAggregators.decorate(
        query.getPostAggregatorSpecs(),
        query.getAggregatorSpecs()
    );

    return new Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>>()
    {
      @Override
      public Result<TimeseriesResultValue> apply(Result<TimeseriesResultValue> result)
      {
        final TimeseriesResultValue holder = result.getValue();
        final Map<String, Object> values = Maps.newLinkedHashMap(holder.getBaseObject());
        if (calculatePostAggs) {
          // put non finalized aggregators for calculating dependent post Aggregators
          for (AggregatorFactory agg : query.getAggregatorSpecs()) {
            values.put(agg.getName(), holder.getMetric(agg.getName()));
          }
          for (PostAggregator postAgg : postAggregators) {
            values.put(postAgg.getName(), postAgg.compute(result.getTimestamp(), values));
          }
        }
        for (AggregatorFactory agg : query.getAggregatorSpecs()) {
          values.put(agg.getName(), fn.manipulate(agg, holder.getMetric(agg.getName())));
        }

        return new Result<TimeseriesResultValue>(
            result.getTimestamp(),
            new TimeseriesResultValue(values)
        );
      }
    };
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> finalQueryDecoration(final QueryRunner<Result<TimeseriesResultValue>> runner)
  {
    return new QueryRunner<Result<TimeseriesResultValue>>()
    {
      @Override
      public Sequence<Result<TimeseriesResultValue>> run(
          Query<Result<TimeseriesResultValue>> query, Map<String, Object> responseContext
      )
      {
        TimeseriesQuery timeseries = (TimeseriesQuery) query;
        final List<String> outputColumns = timeseries.getOutputColumns();
        final LateralViewSpec lateralViewSpec = timeseries.getLateralView();

        Sequence<Result<TimeseriesResultValue>> sequence;
        if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
          sequence = Sequences.map(
              runner.run(query, responseContext), new Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>>()
              {
                @Override
                public Result<TimeseriesResultValue> apply(Result<TimeseriesResultValue> input)
                {
                  DateTime timestamp = input.getTimestamp();
                  TimeseriesResultValue value = input.getValue();
                  Map<String, Object> original = value.getBaseObject();
                  Map<String, Object> retained = Maps.newHashMapWithExpectedSize(outputColumns.size());
                  for (String retain : outputColumns) {
                    retained.put(retain, original.get(retain));
                  }
                  return new Result<>(timestamp, new TimeseriesResultValue(retained));
                }
              }
          );
        } else {
          sequence = runner.run(query, responseContext);
        }
        if (!LimitSpecs.isDummy(timeseries.getLimitSpec()) || timeseries.getHavingSpec() != null) {
          // one row per time granularity.. no mean on ordering with time..
          // todo user can provide coarser granularity for time ordering
          sequence = Queries.convertBack(query, timeseries.applyLimit(Queries.convertToRow(query, sequence), false));
        }
        return lateralViewSpec != null ? toLateralView(sequence, lateralViewSpec) : sequence;
      }
    };
  }

  Sequence<Result<TimeseriesResultValue>> toLateralView(
      Sequence<Result<TimeseriesResultValue>> result, final LateralViewSpec lateralViewSpec
  )
  {
    return Sequences.concat(
        Sequences.map(
            result, new Function<Result<TimeseriesResultValue>, Sequence<Result<TimeseriesResultValue>>>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Sequence<Result<TimeseriesResultValue>> apply(Result<TimeseriesResultValue> input)
              {
                final DateTime timestamp = input.getTimestamp();
                final Map<String, Object> event = input.getValue().getBaseObject();
                return Sequences.simple(
                    Iterables.transform(
                        lateralViewSpec.apply(event),
                        new Function<Map<String, Object>, Result<TimeseriesResultValue>>()
                        {
                          @Override
                          public Result<TimeseriesResultValue> apply(Map<String, Object> input)
                          {
                            return new Result(timestamp, new TimeseriesResultValue(input));
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
  public <I> QueryRunner<Result<TimeseriesResultValue>> handleSubQuery(
      final QueryRunner<I> subQueryRunner,
      final QuerySegmentWalker segmentWalker,
      final ExecutorService executor,
      final int maxRowCount
  )
  {
    return new SubQueryRunner<I>(subQueryRunner, segmentWalker, executor, maxRowCount)
    {
      @Override
      protected Function<Interval, Sequence<Result<TimeseriesResultValue>>> function(
          final Query<Result<TimeseriesResultValue>> query, Map<String, Object> context,
          final Segment segment
      )
      {
        final TimeseriesQueryEngine engine = new TimeseriesQueryEngine();
        final TimeseriesQuery outerQuery = (TimeseriesQuery) query;
        return new Function<Interval, Sequence<Result<TimeseriesResultValue>>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> apply(Interval interval)
          {
            return engine.process(
                outerQuery.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)),
                segment,
                null
            );
          }
        };
      }
    };
  }
}
