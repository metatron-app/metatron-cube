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
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import io.druid.cache.Cache;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import io.druid.segment.SegmentMissingException;
import io.druid.segment.StorageAdapter;

import java.util.List;
import java.util.Map;

/**
 */
public class TimeseriesQueryEngine
{
  public Sequence<Row> process(TimeseriesQuery query, Segment segment, boolean compact)
  {
    return process(query, segment, compact, null);
  }

  public Sequence<Row> process(
      final TimeseriesQuery query,
      final Segment segment,
      final boolean compact,
      final Cache cache
  )
  {
    final StorageAdapter adapter = segment.asStorageAdapter(true);
    if (adapter == null) {
      throw new SegmentMissingException(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query,
        cache,
        new Function<Cursor, Row>()
        {
          private final boolean skipEmptyBuckets = query.isSkipEmptyBuckets();
          private final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
          private final String[] aggregatorNames = AggregatorFactory.toNamesAsArray(aggregatorSpecs);

          @Override
          public Row apply(Cursor cursor)
          {

            if (skipEmptyBuckets && cursor.isDone()) {
              return null;
            }

            final Aggregator[] aggregators = AggregatorFactory.toAggregatorsAsArray(cursor, aggregatorSpecs);
            try {
              while (!cursor.isDone()) {
                for (Aggregator aggregator : aggregators) {
                  aggregator.aggregate();
                }
                cursor.advance();
              }

              if (compact) {
                final Object[] array = new Object[aggregators.length + 1];
                for (int i = 1; i < array.length; i++) {
                  array[i] = aggregators[i - 1].get();
                }
                array[0] = cursor.getTime().getMillis();
                return new CompactRow(array);
              } else {
                final Map<String, Object> event = Maps.newLinkedHashMap();
                for (int i = 0; i < aggregators.length; i++) {
                  event.put(aggregatorNames[i], aggregators[i].get());
                }

                return new MapBasedRow(cursor.getTime(), event);
              }
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
