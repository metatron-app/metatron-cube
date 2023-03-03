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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.inject.Inject;
import io.druid.common.guava.Sequence;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseAggregationQueryToolChest;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.Map;

/**
 */
public class TimeseriesQueryQueryToolChest extends BaseAggregationQueryToolChest<TimeseriesQuery>
{
  private final TimeseriesQueryMetricsFactory metricsFactory;

  @Inject
  public TimeseriesQueryQueryToolChest(TimeseriesQueryMetricsFactory metricsFactory)
  {
    this.metricsFactory = metricsFactory;
  }

  @VisibleForTesting
  public TimeseriesQueryQueryToolChest()
  {
    this(DefaultTimeseriesQueryMetricsFactory.instance());
  }

  @Override
  protected byte queryCode()
  {
    return TIMESERIES_QUERY;
  }

  @Override
  protected Comparator<Row> getMergeOrdering(TimeseriesQuery timeseries)
  {
    final Granularity granularity = timeseries.getGranularity();
    if (Granularities.ALL.equals(granularity)) {
      return null;  // accumulate all
    }
    return new Comparator<Row>()
    {
      @Override
      public int compare(Row o1, Row o2)
      {
        return Long.compare(
            granularity.bucketStart(o1.getTimestamp()).getMillis(),
            granularity.bucketStart(o2.getTimestamp()).getMillis()
        );
      }
    };
  }

  @Override
  public TimeseriesQueryMetrics makeMetrics(Query<Row> query)
  {
    TimeseriesQueryMetrics queryMetrics = metricsFactory.makeMetrics();
    queryMetrics.query(query);
    return queryMetrics;
  }

  @Override
  public <I> QueryRunner<Row> handleSubQuery(QuerySegmentWalker segmentWalker)
  {
    return new SubQueryRunner<I>(segmentWalker)
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        QueryDataSource dataSource = (QueryDataSource) query.getDataSource();
        if (Granularities.isAll(query.getGranularity()) || QueryUtils.isTimeSorted(dataSource.getQuery())) {
          return runStreaming(query, responseContext);
        }
        return super.run(query, responseContext);
      }

      @Override
      protected Function<Interval, Sequence<Row>> query(Query<Row> query, Segment segment)
      {
        final TimeseriesQuery timeseries = (TimeseriesQuery) query;
        return new Function<Interval, Sequence<Row>>()
        {
          @Override
          public Sequence<Row> apply(Interval interval)
          {
            return postAggregation(timeseries, new TimeseriesQueryEngine().process(timeseries, segment, false));
          }
        };
      }

      @Override
      protected Function<Cursor, Sequence<Row>> streamQuery(Query<Row> query)
      {
        final TimeseriesQuery timeseries = (TimeseriesQuery) query;
        return new Function<Cursor, Sequence<Row>>()
        {
          @Override
          public Sequence<Row> apply(Cursor input)
          {
            return postAggregation(timeseries, TimeseriesQueryEngine.processor(timeseries, false).apply(input));
          }
        };
      }
    };
  }
}
