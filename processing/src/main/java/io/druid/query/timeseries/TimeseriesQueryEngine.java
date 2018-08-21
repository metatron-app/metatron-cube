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

import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import io.druid.cache.Cache;
import io.druid.query.BaseQuery;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import io.druid.segment.SegmentMissingException;
import io.druid.segment.StorageAdapter;

import java.util.List;

/**
 */
public class TimeseriesQueryEngine
{
  public Sequence<Result<TimeseriesResultValue>> process(final TimeseriesQuery query, final Segment segment)
  {
    return process(query, segment, null);
  }

  public Sequence<Result<TimeseriesResultValue>> process(
      final TimeseriesQuery query,
      final Segment segment,
      final Cache cache
  )
  {
    final StorageAdapter adapter = segment.asStorageAdapter(true);
    if (adapter == null) {
      throw new SegmentMissingException(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }
    RowResolver resolver = RowResolver.of(adapter, BaseQuery.getVirtualColumns(query));

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query.getQuerySegmentSpec().getIntervals(),
        resolver,
        query.getDimFilter(),
        cache,
        query.isDescending(),
        query.getGranularity(),
        new Function<Cursor, Result<TimeseriesResultValue>>()
        {
          private final boolean skipEmptyBuckets = query.isSkipEmptyBuckets();
          private final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();

          @Override
          public Result<TimeseriesResultValue> apply(Cursor cursor)
          {
            final String[] aggregatorNames = QueryRunnerHelper.makeAggregatorNames(aggregatorSpecs);
            final Aggregator[] aggregators = QueryRunnerHelper.makeAggregators(cursor, aggregatorSpecs);

            if (skipEmptyBuckets && cursor.isDone()) {
              return null;
            }

            try {
              while (!cursor.isDone()) {
                for (Aggregator aggregator : aggregators) {
                  aggregator.aggregate();
                }
                cursor.advance();
              }

              TimeseriesResultBuilder bob = new TimeseriesResultBuilder(cursor.getTime());

              for (int i = 0; i < aggregators.length; i++) {
                bob.addMetric(aggregatorNames[i], aggregators[i]);
              }

              return bob.build();
            }
            finally {
              // cleanup
              for (Aggregator agg : aggregators) {
                agg.close();
              }
            }
          }
        }
    );
  }
}
