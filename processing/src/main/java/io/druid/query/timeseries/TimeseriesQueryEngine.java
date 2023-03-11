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
import com.google.common.collect.Maps;
import io.druid.cache.SessionCache;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import io.druid.segment.filter.FilterContext;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Arrays;
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
      final SessionCache cache
  )
  {
    return QueryRunnerHelper.makeCursorBasedQueryConcat(segment, query, cache, processor(query, compact));
  }

  public static Function<Cursor, Sequence<Row>> processor(final TimeseriesQuery query, final boolean compact)
  {
    return new Function<Cursor, Sequence<Row>>()
    {
      private final Granularity granularity = query.getGranularity();
      private final List<String> columns = query.estimatedInitialColumns();
      private final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
      private final String[] aggregatorNames = AggregatorFactory.toNamesAsArray(aggregatorSpecs);
      private final boolean countAll = Aggregators.isCountAll(aggregatorSpecs);

      @Override
      public Sequence<Row> apply(final Cursor cursor)
      {
        if (cursor.isDone()) {
          return Sequences.empty(columns);
        }
        FilterContext context = cursor.getFilterContext();
        if (countAll && context != null && context.isFullScan()) {
          final long timestamp = BaseQuery.getUniversalTimestamp(query, cursor.getStartTime());
          final Row row;
          if (compact) {
            row = new CompactRow(new Object[] {new MutableLong(timestamp), context.targetNumRows()});
          } else {
            row = new MapBasedRow(timestamp, GuavaUtils.mutableMap(aggregatorNames[0], context.targetNumRows()));
          }
          return Sequences.simple(columns, Arrays.asList(row));
        }
        if (Granularities.isAll(granularity)) {
          return Sequences.simple(columns, new Iterable<Row>()
          {
            private final long timestamp = BaseQuery.getUniversalTimestamp(query, cursor.getStartTime());
            private final Aggregator[] aggregators = Aggregators.makeAggregators(aggregatorSpecs, cursor);

            @Override
            public Iterator<Row> iterator()
            {
              return new CloseableIterator<Row>()
              {
                @Override
                public void close() throws IOException
                {
                  Aggregators.close(aggregators);
                }

                @Override
                public boolean hasNext()
                {
                  return !cursor.isDone();
                }

                @Override
                public Row next()
                {
                  final Object[] values = new Object[aggregators.length];
                  while (!cursor.isDone()) {
                    Aggregators.aggregate(values, aggregators);
                    cursor.advance();
                  }
                  if (compact) {
                    return asCompact(timestamp, values, aggregators);
                  } else {
                    return asMap(DateTimes.utc(timestamp), values, aggregators);
                  }
                }
              };
            }
          });
        }
        return Sequences.simple(columns, new Iterable<Row>()
        {
          private final Aggregator[] aggregators = Aggregators.makeAggregators(aggregatorSpecs, cursor);
          private final Object[] values = new Object[aggregators.length];

          @Override
          public Iterator<Row> iterator()
          {
            return new CloseableIterator<Row>()
            {
              private DateTime prev;

              @Override
              public void close() throws IOException
              {
                Aggregators.close(aggregators);
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
                  final long current = granularity.bucketStart(cursor.getRowTimestamp());
                  if (prev == null) {
                    prev = granularity.toDateTime(current);
                  } else if (prev.getMillis() != current) {
                    Row row = flushRow(prev, values, aggregators);
                    prev = granularity.toDateTime(current);
                    return row;
                  }
                  Aggregators.aggregate(values, aggregators);
                  cursor.advance();
                }
                return flushRow(prev, values, aggregators);
              }
            };
          }
        });
      }

      private Row flushRow(DateTime current, Object[] values, Aggregator[] aggregators)
      {
        if (compact) {
          return asCompact(current.getMillis(), values, aggregators);
        } else {
          return asMap(current, values, aggregators);
        }
      }

      @SuppressWarnings("unchecked")
      private Row asCompact(final long timestamp, final Object[] values, final Aggregator[] aggregators)
      {
        final Object[] array = new Object[values.length + 1];
        array[0] = new MutableLong(timestamp);
        for (int i = 0; i < aggregators.length; i++) {
          array[i + 1] = aggregators[i].get(values[i]);
        }
        Arrays.fill(values, null);
        return new CompactRow(array);
      }

      @SuppressWarnings("unchecked")
      private Row asMap(final DateTime timestamp, final Object[] values, final Aggregator[] aggregators)
      {
        final Map<String, Object> event = Maps.newLinkedHashMap();
        for (int i = 0; i < aggregators.length; i++) {
          event.put(aggregatorNames[i], aggregators[i].get(values[i]));
        }
        Arrays.fill(values, null);
        return new MapBasedRow(timestamp, event);
      }
    };
  }
}
