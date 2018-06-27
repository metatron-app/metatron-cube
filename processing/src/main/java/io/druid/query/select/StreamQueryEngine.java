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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.cache.Cache;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.StorageAdapter;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

/**
 */
public class StreamQueryEngine
{
  public Sequence<StreamQueryRow> process(
      final StreamQuery query,
      final Segment segment,
      final Future optimizer,
      final Cache cache
  )
  {
    final Pair<Schema, Sequence<Object[]>> result = processRaw(query, segment, optimizer, cache);
    final String[] columnNames = query.getColumns().toArray(new String[0]);
    return Sequences.map(
        result.rhs, new Function<Object[], StreamQueryRow>()
        {
          @Override
          public StreamQueryRow apply(final Object[] input)
          {
            final StreamQueryRow theEvent = new StreamQueryRow();
            for (int i = 0; i < input.length; i++) {
              theEvent.put(columnNames[i], input[i]);
            }
            return theEvent;
          }
        }
    );
  }

  public Sequence<RawRows> process(
      final StreamRawQuery query,
      final Segment segment,
      final Future optimizer,
      final Cache cache
  )
  {
    Pair<Schema, Sequence<Object[]>> result = processRaw(query, segment, optimizer, cache);
    List<Object[]> rows = Sequences.toList(result.rhs, Lists.<Object[]>newArrayList());
    if (rows.isEmpty()) {
      return Sequences.empty();
    }
    DateTime start = segment.getDataInterval().getStart();
    return Sequences.simple(Arrays.asList(new RawRows(start, result.lhs, rows)));
  }

  public Pair<Schema, Sequence<Object[]>> processRaw(
      final AbstractStreamQuery<?> query,
      final Segment segment,
      final Future optimizer,
      final Cache cache
  )
  {
    final StorageAdapter adapter = segment.asStorageAdapter(true);
    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }
    List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final int limit = query.getLimit();
    final String concatString = query.getConcatString();

    final RowResolver resolver = Segments.getResolver(segment, query);
    final List<String> columns = Lists.newArrayList(query.getColumns());

    @SuppressWarnings("unchecked")
    final MutableInt counter = Futures.<MutableInt>getUnchecked(optimizer);

    Sequence<Object[]> sequence = Sequences.concat(
        QueryRunnerHelper.makeCursorBasedQuery(
            adapter,
            Iterables.getOnlyElement(intervals),
            resolver,
            query.getDimensionsFilter(),
            cache,
            query.isDescending(),
            query.getGranularity(),
            new Function<Cursor, Sequence<Object[]>>()
            {
              @Override
              public Sequence<Object[]> apply(final Cursor cursor)
              {
                int index = 0;
                final ObjectColumnSelector[] selectors = new ObjectColumnSelector[columns.size()];
                for (String column : columns) {
                  if (resolver.isDimension(column)) {
                    DimensionSelector selector = cursor.makeDimensionSelector(DefaultDimensionSpec.of(column));
                    if (concatString != null) {
                      selectors[index++] = ColumnSelectors.asConcatValued(selector, concatString);
                    } else {
                      selectors[index++] = ColumnSelectors.asMultiValued(selector);
                    }
                  } else {
                    selectors[index++] = cursor.makeObjectColumnSelector(column);
                  }
                }

                return Sequences.simple(
                    new Iterable<Object[]>()
                    {
                      @Override
                      public Iterator<Object[]> iterator()
                      {
                        return new Iterator<Object[]>()
                        {
                          @Override
                          public boolean hasNext()
                          {
                            return !cursor.isDone() && (limit <= 0 || counter.intValue() < limit);
                          }

                          @Override
                          public Object[] next()
                          {
                            final Object[] theEvent = new Object[selectors.length];

                            int index = 0;
                            for (ObjectColumnSelector selector : selectors) {
                              theEvent[index++] = selector == null ? null : selector.get();
                            }
                            counter.increment();
                            cursor.advance();
                            return theEvent;
                          }
                        };
                      }
                    }
                );
              }
            }
        )
    );

    return Pair.of(resolver.toSubSchema(columns), sequence);
  }
}
