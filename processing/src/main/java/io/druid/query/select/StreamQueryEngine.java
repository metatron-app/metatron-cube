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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import io.druid.cache.Cache;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.TopNSorter;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.OrderingSpec;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.SingleValued;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/**
 */
public class StreamQueryEngine
{
  private static final Logger LOG = new Logger(StreamQueryEngine.class);

  public Sequence<Object[]> process(
      final StreamQuery query,
      final QueryConfig config,
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
    return QueryRunnerHelper.makeCursorBasedQueryConcat(
        adapter,
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
      private final int limit = orderings.isEmpty() ? query.getSimpleLimit() : -1;

      @Override
      public Sequence<Object[]> apply(final Cursor cursor)
      {
        final List<String> orderingColumns = Lists.newArrayList(Iterables.transform(orderings, o -> o.getDimension()));
        final Set<String> orderingColumnSet = Sets.newHashSet(orderingColumns);
        int index = 0;
        final DimensionSelector[] dimSelectors = new DimensionSelector[columns.length];
        final ObjectColumnSelector[] selectors = new ObjectColumnSelector[columns.length];
        for (String column : columns) {
          boolean orderingColumn = orderingColumnSet.contains(column);
          if (cursor.resolve(column, ValueDesc.UNKNOWN).isDimension()) {
            final DimensionSelector selector = cursor.makeDimensionSelector(DefaultDimensionSpec.of(column));
            if (selector instanceof SingleValued) {
              if (selector.withSortedDictionary()) {
                orderingColumnSet.remove(column);
                dimSelectors[index] = selector;
              }
              if (useRawUTF8 && selector instanceof WithRawAccess) {
                selectors[index++] = ColumnSelectors.asRawAccess((WithRawAccess) selector);
              } else {
                selectors[index++] = ColumnSelectors.asSingleValued((SingleValued) selector);
              }
            } else if (concatString != null) {
              selectors[index++] = ColumnSelectors.asConcatValued(selector, concatString);
            } else {
              selectors[index++] = ColumnSelectors.asMultiValued(selector);
            }
          } else {
            selectors[index++] = cursor.makeObjectColumnSelector(column);
          }
        }

        if (!orderings.isEmpty() && orderingColumnSet.isEmpty() && OrderingSpec.isAllNaturalOrdering(orderings)) {
          final IntComparator[] comparators = new IntComparator[orderingColumns.size()];
          for (int i = 0; i < comparators.length; i++) {
            if (orderings.get(i).getDirection() == Direction.DESCENDING) {
              comparators[i] = (l, r) -> Integer.compare(r, l);
            } else {
              comparators[i] = (l, r) -> Integer.compare(l, r);
            }
          }
          final int[] indices = GuavaUtils.indexOf(orderingColumns, query.getColumns());
          final List<int[]> keys = Lists.newArrayList();
          final List<Object[]> values = Lists.newArrayList();
          for (int x = 0; !cursor.isDone(); x++) {
            final int[] key = new int[comparators.length + 1];
            final Object[] value = new Object[selectors.length];
            for (int i = 0; i < selectors.length; i++) {
              if (indices[i] >= 0) {
                key[indices[i]] = dimSelectors[i].getRow().get(0);
              }
              value[i] = selectors[i] == null ? null : selectors[i].get();
            }
            key[comparators.length] = x;
            keys.add(key);
            values.add(value);
            cursor.advance();
          }

          final Iterator<int[]> sorted;
          if (limit <= 0 || keys.size() < limit) {
            Collections.sort(keys, toComparator(comparators));
            sorted = keys.iterator();
          } else {
            sorted = TopNSorter.topN(toComparator(comparators), keys, limit);
          }
          return Sequences.once(
              query.getColumns(), Iterators.transform(sorted, key -> values.get(key[comparators.length]))
          );
        }

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
          sequence = LimitSpec.sortLimit(sequence, query.getMergeOrdering(), -1);
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
