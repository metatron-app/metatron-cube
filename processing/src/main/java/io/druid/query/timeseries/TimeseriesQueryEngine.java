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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.parsers.CloseableIterator;
import io.druid.cache.Cache;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.RowResolver;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import io.druid.segment.SegmentMissingException;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Iterator;
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
    return Sequences.explode(
        adapter.makeCursors(
            query.getDimFilter(),
            Iterables.getOnlyElement(query.getIntervals()),
            RowResolver.of(adapter, BaseQuery.getVirtualColumns(query)),
            Granularities.ALL,
            query.isDescending(), cache
        ),
        processor(query, compact)
    );
  }

  public static Function<Cursor, Sequence<Row>> processor(final TimeseriesQuery query, final boolean compact)
  {
    return new Function<Cursor, Sequence<Row>>()
    {
      private final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
      private final String[] aggregatorNames = AggregatorFactory.toNamesAsArray(aggregatorSpecs);

      @Override
      public Sequence<Row> apply(final Cursor cursor)
      {
        if (cursor.isDone()) {
          return Sequences.empty();
        }
        final Granularity granularity = query.getGranularity();
        final Aggregator[] aggregators = AggregatorFactory.toAggregatorsAsArray(cursor, aggregatorSpecs);
        if (granularity == Granularities.ALL) {
          return Sequences.simple(new Iterable<Row>()
          {
            @Override
            public Iterator<Row> iterator()
            {
              return new CloseableIterator<Row>()
              {
                @Override
                public void close() throws IOException
                {
                  for (Aggregator agg : aggregators) {
                    agg.close();
                  }
                }

                @Override
                public boolean hasNext()
                {
                  return !cursor.isDone();
                }

                @Override
                public Row next()
                {
                  while (!cursor.isDone()) {
                    for (Aggregator aggregator : aggregators) {
                      aggregator.aggregate();
                    }
                    cursor.advance();
                  }
                  return flushRow(cursor.getTime(), aggregators);
                }
              };
            }
          });
        }
        return Sequences.simple(new Iterable<Row>()
        {
          @Override
          public Iterator<Row> iterator()
          {
            return new CloseableIterator<Row>()
            {
              private DateTime prev;

              @Override
              public void close() throws IOException
              {
                for (Aggregator agg : aggregators) {
                  agg.close();
                }
              }

              @Override
              public boolean hasNext()
              {
                return !cursor.isDone();
              }

              @Override
              public Row next()
              {
                for (Aggregator agg : aggregators) {
                  agg.reset();
                }
                while (!cursor.isDone()) {
                  final DateTime current = granularity.bucketStart(cursor.getRowTime());
                  if (prev == null) {
                    prev = current;
                  } else if (prev.getMillis() != current.getMillis()) {
                    Row row = flushRow(prev, aggregators);
                    prev = current;
                    return row;
                  }
                  for (Aggregator aggregator : aggregators) {
                    aggregator.aggregate();
                  }
                  cursor.advance();
                }
                return flushRow(prev, aggregators);
              }
            };
          }
        });
      }

      private Row flushRow(DateTime current, Aggregator[] aggregators)
      {
        if (compact) {
          final Object[] array = new Object[aggregators.length + 1];
          for (int i = 1; i < array.length; i++) {
            array[i] = aggregators[i - 1].get();
          }
          array[0] = current.getMillis();
          return new CompactRow(array);
        } else {
          final Map<String, Object> event = Maps.newLinkedHashMap();
          for (int i = 0; i < aggregators.length; i++) {
            event.put(aggregatorNames[i], aggregators[i].get());
          }
          return new MapBasedRow(current, event);
        }
      }
    };
  }
}
