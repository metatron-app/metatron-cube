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
import io.druid.cache.SessionCache;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DictionaryID;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderedPriorityQueueItems;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.OrderingSpec;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.Cursors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.Scannable;
import io.druid.segment.DimensionSelector.SingleValued;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.ScanContext;
import io.druid.segment.Segment;
import io.druid.utils.HeapSort;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.LongSupplier;

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
      final SessionCache cache
  )
  {
    return QueryRunnerHelper.makeCursorBasedQueryConcat(
        segment,
        query,
        cache,
        processor(query, config, optimizer == null ? null : (MutableInt) optimizer.get())
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
      private final TableFunctionSpec tableFunction = query.getTableFunction();
      private final List<OrderByColumnSpec> orderings = query.getOrderingSpecs();
      private final String[] columns = query.getColumns().toArray(new String[0]);

      private final boolean useRawUTF8 = config.useUTF8(query);
      private final String concatString = query.getConcatString();

      @Override
      public Sequence<Object[]> apply(Cursor source)
      {
        final Cursor cursor = Cursors.explode(source, tableFunction);

        final ScanContext context = source.scanContext();
        final int numRows = context.awareTargetRows() ? context.count() : context.numRows();
        final List<String> orderingColumns = Lists.newArrayList(Iterables.transform(orderings, o -> o.getDimension()));

        boolean optimizeOrdering = !orderingColumns.isEmpty() && OrderingSpec.isAllNaturalOrdering(orderings);
        final DimensionSelector[] dimensions = new DimensionSelector[columns.length];
        final ObjectColumnSelector[] selectors = new ObjectColumnSelector[columns.length];
        for (int i = 0; i < columns.length; i++) {
          String column = columns[i];
          ValueDesc columnType = cursor.resolve(column, ValueDesc.UNKNOWN);
          if (columnType.isDimension()) {
            final DimensionSelector selector = cursor.makeDimensionSelector(DefaultDimensionSpec.of(column));
            if (selector instanceof SingleValued) {
              if (selector.withSortedDictionary()) {
                dimensions[i] = selector;
              }
              if (useRawUTF8 && columnType.isStringOrDimension()) {
                if (selector instanceof Scannable) {
                  selectors[i] = ColumnSelectors.withRawAccess((Scannable) selector, cursor.scanContext(), column);
                } else {
                  selectors[i] = ColumnSelectors.asSingleRaw((SingleValued) selector);
                }
              } else {
                selectors[i] = ColumnSelectors.asSingleString((SingleValued) selector);
              }
            } else if (concatString != null) {
              selectors[i] = ColumnSelectors.asConcatValued(selector, concatString);
            } else {
              selectors[i] = ColumnSelectors.asMultiValued(selector);
            }
          } else {
            selectors[i] = cursor.makeObjectColumnSelector(column);
          }
          if (orderingColumns.contains(column)) {
            optimizeOrdering &= dimensions[i] != null;
          }
        }

        final int[] indices = GuavaUtils.indexOf(query.getColumns(), orderingColumns, true);
        if (optimizeOrdering && indices != null) {
          final int limit = query.getSimpleLimit();
          // optimize order by dimensions only
          final Direction[] directions = OrderingSpec.getDirections(orderings).toArray(new Direction[0]);
          final DimensionSelector[] orders = GuavaUtils.collect(dimensions, indices).toArray(new DimensionSelector[0]);

          final int[] cardinalities = Arrays.stream(orders).mapToInt(DimensionSelector::getValueCardinality).toArray();
          final int[] bits = DictionaryID.bitsRequired(cardinalities);
          if (numRows > 0 && bits != null) {
            final int keyBits = Arrays.stream(bits).sum();
            final int rowBits = DictionaryID.bitsRequired(numRows);
            if (keyBits + rowBits < Long.SIZE) {
              final long[] keys = new long[numRows];
              final Object[][] values = new Object[numRows][selectors.length];
              final LongSupplier supplier = keys(orders, directions, cardinalities, DictionaryID.bitsToShifts(bits));
              int ix = 0;
              for (; !cursor.isDone(); cursor.advance(), ix++) {
                keys[ix] = (supplier.getAsLong() << rowBits) + ix;
                values[ix] = values(selectors);
              }
              Arrays.sort(keys, 0, ix);

              final int valid = ix;
              final Sequence<Object[]> sequence = Sequences.once(query.getColumns(), new Iterator<Object[]>()
              {
                private final int mask = DictionaryID.bitToMask(rowBits);
                private int index;

                @Override
                public boolean hasNext()
                {
                  return index < valid;
                }

                @Override
                public Object[] next()
                {
                  return values[(int) (keys[index++] & mask)];
                }
              });
              return limit > 0 ? Sequences.limit(sequence, limit) : sequence;
            } else if (keyBits < Long.SIZE) {
              final long[] keys = new long[numRows];
              final int[] rows = new int[numRows];
              final Object[][] values = new Object[numRows][selectors.length];
              final LongSupplier supplier = keys(orders, directions, cardinalities, DictionaryID.bitsToShifts(bits));
              int ix = 0;
              for (; !cursor.isDone(); cursor.advance(), ix++) {
                keys[ix] = supplier.getAsLong();
                rows[ix] = ix;
                values[ix] = values(selectors);
              }
              HeapSort.sort(keys, rows, 0, ix);

              final int valid = ix;
              final Sequence<Object[]> sequence = Sequences.once(query.getColumns(), new Iterator<Object[]>()
              {
                private int index;

                @Override
                public boolean hasNext()
                {
                  return index < valid;
                }

                @Override
                public Object[] next()
                {
                  return values[rows[index++]];
                }
              });
              return limit > 0 ? Sequences.limit(sequence, limit) : sequence;
            }
          }

          final Comparator<int[]> comparator = comparator(directions);
          final Queue<int[]> keys = limit > 0
                                    ? MinMaxPriorityQueue.orderedBy(comparator).maximumSize(limit).create()
                                    : new PriorityQueue<>(comparator);
          final List<Object[]> values = Lists.newArrayList();
          while (!cursor.isDone()) {
            final int[] key = new int[orders.length + 1];
            for (int i = 0; i < orders.length; i++) {
              key[i] = orders[i].getRow().get(0);
            }
            if (keys.offer(key)) {
              key[orders.length] = values.size();
              values.add(values(selectors));
            }
            cursor.advance();
          }
          final Iterator<int[]> sorted = new OrderedPriorityQueueItems<int[]>(keys);
          return Sequences.once(
              query.getColumns(), Iterators.transform(sorted, key -> values.get(key[orders.length]))
          );
        }

        final int limit = orderings.isEmpty() ? query.getSimpleLimit() : -1;
        Sequence<Object[]> sequence = Sequences.once(
            query.getColumns(),
            new Iterator<Object[]>()
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
            }
        );
        if (!orderings.isEmpty()) {
          sequence = LimitSpec.sortLimit(sequence, query.getMergeOrdering(sequence.columns()), -1);
        }
        return sequence;
      }
    };
  }

  private static Object[] values(final ObjectColumnSelector[] selectors)
  {
    final Object[] value = new Object[selectors.length];
    for (int i = 0; i < selectors.length; i++) {
      value[i] = selectors[i] == null ? null : selectors[i].get();
    }
    return value;
  }

  private static Comparator<int[]> comparator(Direction[] directions)
  {
    final IntComparator[] comparators = new IntComparator[directions.length];
    for (int i = 0; i < directions.length; i++) {
      comparators[i] = comparator(directions[i]);
    }
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

  private static IntComparator comparator(Direction direction)
  {
    return direction == Direction.ASCENDING ? ((l, r) -> Integer.compare(l, r)) : ((l, r) -> Integer.compare(r, l));
  }

  private static long flip(long a)
  {
    return a ^ Long.MIN_VALUE;
  }

  private static LongSupplier keys(DimensionSelector[] selectors, Direction[] directions, int[] cardinalities, int[] shifts)
  {
    if (!Arrays.asList(directions).contains(Direction.DESCENDING)) {
      if (selectors.length == 1) {
        return () -> (long) selectors[0].getRow().get(0) << shifts[0];
      }
      return () -> {
        long keys = 0;
        for (int i = 0; i < selectors.length; i++) {
          keys += (long) selectors[i].getRow().get(0) << shifts[i];
        }
        return keys;
      };
    }
    return () -> {
      long keys = 0;
      for (int i = 0; i < selectors.length; i++) {
        int v = selectors[i].getRow().get(0);
        if (directions[i] == Direction.DESCENDING) {
          v = cardinalities[i] - v;
        }
        keys += (long) v << shifts[i];
      }
      return keys;
    };
  }
}
