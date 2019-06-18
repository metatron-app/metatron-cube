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

package io.druid.query.timeseries;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseAggregationQueryToolChest;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.Cursor;

import java.util.Comparator;
import java.util.List;

/**
 */
public class TimeseriesQueryQueryToolChest extends BaseAggregationQueryToolChest<TimeseriesQuery>
{
  @Inject
  public TimeseriesQueryQueryToolChest(IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator)
  {
    super(intervalChunkingQueryRunnerDecorator);
  }

  @Override
  protected byte queryCode()
  {
    return TIMESERIES_QUERY;
  }

  @Override
  protected Ordering<Row> getMergeOrdering(TimeseriesQuery timeseries)
  {
    final Granularity granularity = timeseries.getGranularity();
    if (Granularities.ALL.equals(granularity)) {
      return GuavaUtils.allEquals();
    }
    return Ordering.from(new Comparator<Row>()
    {
      @Override
      public int compare(Row o1, Row o2)
      {
        return Longs.compare(
            granularity.bucketStart(o1.getTimestamp()).getMillis(),
            granularity.bucketStart(o2.getTimestamp()).getMillis()
        );
      }
    });
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
  public <I> QueryRunner<Row> handleSubQuery(QuerySegmentWalker segmentWalker, QueryConfig config)
  {
    return new StreamingSubQueryRunner<I>(segmentWalker, config)
    {
      @Override
      protected Function<Cursor, Sequence<Row>> streamQuery(Query<Row> query)
      {
        final TimeseriesQuery timeseries = (TimeseriesQuery) query;
        return new Function<Cursor, Sequence<Row>>() {
          @Override
          public Sequence<Row> apply(Cursor input)
          {
            return Sequences.map(
                TimeseriesQueryEngine.processor(timeseries, false).apply(input),
                toPostAggregator(timeseries)
            );
          }
        };
      }
    };
  }
}
