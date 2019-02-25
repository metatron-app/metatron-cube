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
import com.google.common.util.concurrent.Futures;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.cache.Cache;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

/**
 */
public class StreamQueryEngine
{
  public Sequence<Object[]> process(
      final StreamQuery query,
      final Segment segment,
      final Future optimizer,
      final Cache cache
  )
  {
    Sequence<Object[]> result = processRaw(query, segment, optimizer, cache);
    if (GuavaUtils.isNullOrEmpty(query.getOrderBySpecs())) {
      return result;
    }
    List<Object[]> sorted = Sequences.toList(result);
    if (sorted.isEmpty()) {
      return Sequences.empty();
    }
    Collections.sort(sorted, query.getResultOrdering());
    return Sequences.simple(sorted);
  }

  private Sequence<Object[]> processRaw(
      final StreamQuery query,
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

    @SuppressWarnings("unchecked")
    final MutableInt counter = Futures.<MutableInt>getUnchecked(optimizer);
    return Sequences.concat(
        QueryRunnerHelper.makeCursorBasedQuery(
            adapter,
            query,
            cache,
            converter(query, counter)
        )
    );
  }

  public static Function<Cursor, Sequence<Object[]>> converter(
      final StreamQuery query,
      final MutableInt counter
  )
  {
    return new Function<Cursor, Sequence<Object[]>>()
    {
      private final String[] columns = query.getColumns().toArray(new String[0]);
      private final String concatString = query.getConcatString();
      private final int limit = query.getLimit();

      @Override
      public Sequence<Object[]> apply(final Cursor cursor)
      {
        int index = 0;
        final RowResolver resolver = cursor.resolver();
        final ObjectColumnSelector[] selectors = new ObjectColumnSelector[columns.length];
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
    };
  }
}
