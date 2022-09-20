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

package io.druid.query.select;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.cache.Cache;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderedPriorityQueueItems;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.OrderingSpec;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.SingleValued;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 */
public class StreamQueryEngine
{
  private static final Logger LOG = new Logger(StreamQueryEngine.class);

  public Sequence<Object[]> process(
      final StreamQuery query,
      final QueryConfig config,
      final Segment segment,
      final Supplier optimizer,
      final Cache cache
  )
  {
    final MutableInt counter = (MutableInt) optimizer.get();
    return QueryRunnerHelper.makeCursorBasedQueryConcat(
        segment,
        query,
        cache,
        processor(query, config, counter)
    );
  }

  public static Function<Cursor, Sequence<Object[]>> processor(
      final StreamQuery query,
      final QueryConfig config,
      final MutableInt counter
  )
  {
    return new Function<Cursor, Sequence<Object[]>>()
    {
      private final List<OrderByColumnSpec> orderings = query.getOrderingSpecs();
      private final boolean useRawUTF8 = !BaseQuery.isLocalFinalizingQuery(query) &&
                                         query.getContextBoolean(Query.STREAM_USE_RAW_UTF8, config.getSelect().isUseRawUTF8());
      private final String[] columns = query.getColumns().toArray(new String[0]);
      private final String concatString = query.getConcatString();

      @Override
      public Sequence<Object[]> apply(final Cursor cursor)
      {
        final List<String> orderingColumns = Lists.newArrayList(Iterables.transform(orderings, o -> o.getDimension()));
        int index = 0;
        boolean optimizeOrdering = !orderingColumns.isEmpty() && OrderingSpec.isAllNaturalOrdering(orderings);
        final DimensionSelector[] dimSelectors = new DimensionSelector[columns.length];
        final ObjectColumnSelector[] selectors = new ObjectColumnSelector[columns.length];
        for (String column : columns) {
          if (cursor.resolve(column, ValueDesc.UNKNOWN).isDimension()) {
            final DimensionSelector selector = cursor.makeDimensionSelector(DefaultDimensionSpec.of(column));
            if (selector instanceof SingleValued) {
              if (selector.withSortedDictionary()) {
                dimSelectors[index] = selector;
              }
              if (useRawUTF8 && selector instanceof WithRawAccess) {
                selectors[index] = ColumnSelectors.asRawAccess((WithRawAccess) selector);
              } else {
                selectors[index] = ColumnSelectors.asSingleValued((SingleValued) selector);
              }
            } else if (concatString != null) {
              selectors[index] = ColumnSelectors.asConcatValued(selector, concatString);
            } else {
              selectors[index] = ColumnSelectors.asMultiValued(selector);
            }
          } else {
            selectors[index] = cursor.makeObjectColumnSelector(column);
          }
          if (orderingColumns.contains(column)) {
            optimizeOrdering &= dimSelectors[index] != null;
          }
          index++;
        }

        if (optimizeOrdering) {
          // optimize order by dimensions only
          final IntComparator[] comparators = new IntComparator[orderingColumns.size()];
          for (int i = 0; i < comparators.length; i++) {
            if (orderings.get(i).getDirection() == Direction.DESCENDING) {
              comparators[i] = (l, r) -> Integer.compare(r, l);
            } else {
              comparators[i] = (l, r) -> Integer.compare(l, r);
            }
          }

          final Comparator<int[]> comparator = toComparator(comparators);
          final int limit = query.getSimpleLimit();
          final Queue<int[]> keys = limit > 0
                                    ? MinMaxPriorityQueue.orderedBy(comparator).maximumSize(limit).create()
                                    : new PriorityQueue<>(comparator);
          final int[] indices = GuavaUtils.indexOf(query.getColumns(), orderingColumns, true);
          final List<Object[]> values = Lists.newArrayList();
          while (!cursor.isDone()) {
            final int[] key = new int[indices.length + 1];
            for (int i = 0; i < indices.length; i++) {
              key[i] = dimSelectors[indices[i]].getRow().get(0);
            }
            if (keys.offer(key)) {
              key[indices.length] = values.size();
              final Object[] value = new Object[selectors.length];
              for (int i = 0; i < selectors.length; i++) {
                value[i] = selectors[i] == null ? null : selectors[i].get();
              }
              values.add(value);
            }
            cursor.advance();
          }
          final Iterator<int[]> sorted = new OrderedPriorityQueueItems<int[]>(keys);
          return Sequences.once(
              query.getColumns(), Iterators.transform(sorted, key -> values.get(key[comparators.length]))
          );
        }

        final int limit = orderings.isEmpty() ? query.getSimpleLimit() : -1;
        Sequence<Object[]> sequence = Sequences.simple(
            query.getColumns(),
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
        if (!orderings.isEmpty()) {
          sequence = LimitSpec.sortLimit(sequence, query.getMergeOrdering(sequence.columns()), -1);
        }
        return sequence;
      }
    };
  }

  private static Comparator<int[]> toComparator(final IntComparator[] comparators)
  {
    if (comparators.length == 1) {
      return (a, b) -> comparators[0].compare(a[0], b[0]);
    }
    return (a, b) -> {
      for (int i = 0; i < comparators.length; i++) {
        final int compare = comparators[i].compare(a[i], b[i]);
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    };
  }
}
