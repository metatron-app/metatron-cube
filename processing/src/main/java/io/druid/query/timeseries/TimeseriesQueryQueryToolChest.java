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
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.common.guava.CombiningSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IdentityFunction;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValueClass;
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
import io.druid.query.TabularFormat;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TimeseriesQueryQueryToolChest extends QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery>
{
  private static final TypeReference<List<Object>> OBJECT_TYPE_REFERENCE =
      new TypeReference<List<Object>>()
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
      final QueryRunner<Result<TimeseriesResultValue>> runner
  )
  {
    return new QueryRunner<Result<TimeseriesResultValue>>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Result<TimeseriesResultValue>> run(
          Query<Result<TimeseriesResultValue>> query,
          Map<String, Object> responseContext
      )
      {
        TimeseriesQuery timeseries = (TimeseriesQuery) query;
        if (timeseries.getContextBoolean(QueryContextKeys.FINAL_MERGE, true)) {
          Sequence<Result<TimeseriesResultValue>> sequence = runner.run(
              timeseries.removePostActions(),
              responseContext
          );
          if (BaseQuery.getContextBySegment(timeseries)) {
            return Sequences.map((Sequence) sequence, BySegmentResultValueClass.applyAll(
                toPostAggregator(timeseries))
            );
          }
          sequence = CombiningSequence.create(
              sequence, ResultGranularTimestampComparator.create(timeseries), new TimeseriesBinaryFn(timeseries));
          sequence = Sequences.map(sequence, toPostAggregator(timeseries));
          return sequence;
        }
        return runner.run(timeseries, responseContext);
      }
    };
  }

  @SuppressWarnings("unchecked")
  private IdentityFunction<Result<TimeseriesResultValue>> toPostAggregator(TimeseriesQuery timeseries)
  {
    if (GuavaUtils.isNullOrEmpty(timeseries.getPostAggregatorSpecs())) {
      return IdentityFunction.INSTANCE;
    }
    final List<PostAggregator> postAggregators = PostAggregators.decorate(
        timeseries.getPostAggregatorSpecs(),
        timeseries.getAggregatorSpecs()
    );

    return new IdentityFunction<Result<TimeseriesResultValue>>()
    {
      @Override
      public Result<TimeseriesResultValue> apply(Result<TimeseriesResultValue> input)
      {
        final TimeseriesResultValue holder = input.getValue();
        final Map<String, Object> values = holder.getBaseObject();
        for (PostAggregator postAgg : postAggregators) {
          values.put(postAgg.getName(), postAgg.compute(input.getTimestamp(), values));
        }
        return new Result<TimeseriesResultValue>(input.getTimestamp(), new TimeseriesResultValue(values));
      }
    };
  }

  @Override
  public Function<TimeseriesQuery, ServiceMetricEvent.Builder> makeMetricBuilder()
  {
    return new Function<TimeseriesQuery, ServiceMetricEvent.Builder>()
    {
      @Override
      public ServiceMetricEvent.Builder apply(TimeseriesQuery query)
      {
        final List<AggregatorFactory> aggregators = query.getAggregatorSpecs();
        final int numComplexAggs = DruidMetrics.findNumComplexAggs(aggregators);
        return DruidMetrics.makePartialQueryTimeMetric(query)
                           .setDimension("numMetrics", String.valueOf(aggregators.size()))
                           .setDimension("numComplexMetrics", String.valueOf(numComplexAggs));
      }
    };
  }

  @Override
  public TypeReference<Result<TimeseriesResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<TimeseriesResultValue>, List<Object>, TimeseriesQuery> getCacheStrategy(final TimeseriesQuery query)
  {
    return new CacheStrategy<Result<TimeseriesResultValue>, List<Object>, TimeseriesQuery>()
    {
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();

      @Override
      public byte[] computeCacheKey(TimeseriesQuery query)
      {
        final byte[] filterBytes = QueryCacheHelper.computeCacheBytes(query.getDimFilter());
        final byte[] vcBytes = QueryCacheHelper.computeCacheKeys(query.getVirtualColumns());
        final byte[] aggregatorBytes = QueryCacheHelper.computeCacheKeys(query.getAggregatorSpecs());
        final byte[] granularityBytes = query.getGranularity().getCacheKey();
        final byte descending = query.isDescending() ? (byte) 1 : 0;
        final byte skipEmptyBuckets = query.isSkipEmptyBuckets() ? (byte) 1 : 0;

        return ByteBuffer
            .allocate(
                3
                + granularityBytes.length
                + filterBytes.length
                + vcBytes.length
                + aggregatorBytes.length
            )
            .put(TIMESERIES_QUERY)
            .put(descending)
            .put(skipEmptyBuckets)
            .put(granularityBytes)
            .put(filterBytes)
            .put(vcBytes)
            .put(aggregatorBytes)
            .array();
      }

      @Override
      public TypeReference<List<Object>> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TimeseriesResultValue>, List<Object>> prepareForCache()
      {
        return new Function<Result<TimeseriesResultValue>, List<Object>>()
        {
          @Override
          public List<Object> apply(final Result<TimeseriesResultValue> input)
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
      public Function<List<Object>, Result<TimeseriesResultValue>> pullFromCache()
      {
        return new Function<List<Object>, Result<TimeseriesResultValue>>()
        {
          private final Granularity granularity = query.getGranularity();

          @Override
          public Result<TimeseriesResultValue> apply(List<Object> results)
          {
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
  public QueryRunner<Result<TimeseriesResultValue>> preMergeQueryDecoration(QueryRunner<Result<TimeseriesResultValue>> runner)
  {
    return intervalChunkingQueryRunnerDecorator.decorate(runner, this);
  }

  @Override
  public Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makePreComputeManipulatorFn(
      TimeseriesQuery query, MetricManipulationFn fn
  )
  {
    return makeComputeManipulatorFn(query, fn);
  }

  @Override
  public Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makePostComputeManipulatorFn(
      TimeseriesQuery query, MetricManipulationFn fn
  )
  {
    return makeComputeManipulatorFn(query, fn);
  }

  private Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makeComputeManipulatorFn(
      final TimeseriesQuery query, final MetricManipulationFn fn
  )
  {
    if (fn == MetricManipulatorFns.identity() || GuavaUtils.isNullOrEmpty(query.getAggregatorSpecs())) {
      return Functions.identity();
    }
    return new Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>>()
    {
      @Override
      public Result<TimeseriesResultValue> apply(Result<TimeseriesResultValue> result)
      {
        final TimeseriesResultValue holder = result.getValue();
        final Map<String, Object> values = holder.getBaseObject();
        for (AggregatorFactory agg : query.getAggregatorSpecs()) {
          values.put(agg.getName(), fn.manipulate(agg, holder.getMetric(agg.getName())));
        }
        return new Result<TimeseriesResultValue>(result.getTimestamp(), new TimeseriesResultValue(values));
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
              runner.run(query, responseContext),
              new Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>>()
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
  public TabularFormat toTabularFormat(
      final TimeseriesQuery query, final Sequence<Result<TimeseriesResultValue>> sequence, final String timestampColumn
  )
  {
    return new TabularFormat()
    {
      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return Sequences.map(
            sequence, new Function<Result<TimeseriesResultValue>, Map<String, Object>>()
            {
              @Override
              public Map<String, Object> apply(Result<TimeseriesResultValue> input)
              {
                Map<String, Object> row = input.getValue().getBaseObject();
                if (timestampColumn != null) {
                  row.put(timestampColumn, input.getTimestamp());
                }
                return row;
              }
            }
        );
      }

      @Override
      public Map<String, Object> getMetaData()
      {
        return null;
      }
    };
  }

  @Override
  public <I> QueryRunner<Result<TimeseriesResultValue>> handleSubQuery(QuerySegmentWalker segmentWalker, int maxRowCount)
  {
    return new SubQueryRunner<I>(segmentWalker, maxRowCount)
    {
      @Override
      protected Function<Interval, Sequence<Result<TimeseriesResultValue>>> query(
          final Query<Result<TimeseriesResultValue>> query,
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
            return Sequences.map(
                engine.process(outerQuery.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)), segment),
                toPostAggregator(outerQuery)
            );
          }
        };
      }
    };
  }
}
