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

package io.druid.query.aggregation;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import io.druid.common.utils.Murmur3;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.SingleValued;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntToLongFunction;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

public abstract class HashIterator<T extends HashCollector>
{
  protected static final long NULL_HASH = Murmur3.hash64(0);

  protected final ValueMatcher predicate;
  protected final List<DimensionSelector> selectorList;
  protected final int[][] groupings;
  protected final boolean byRow;

  protected final Consumer<T> consumer;

  public HashIterator(ValueMatcher predicate, List<DimensionSelector> selectorList, int[][] groupings, boolean byRow)
  {
    this.predicate = predicate == null ? ValueMatcher.TRUE : predicate;
    this.selectorList = selectorList;
    this.groupings = groupings;
    this.byRow = byRow;
    this.consumer = toConsumer(selectorList);
  }

  // class of collector
  protected abstract Class<T> collectorClass();

  private Consumer<T> toConsumer(List<DimensionSelector> source)
  {
    if (source.isEmpty()) {
      return collector -> collector.collect(NULL_HASH);
    }
    DimensionSelector[] selectors = source.toArray(new DimensionSelector[0]);
    IntToLongFunction[] lookups = source.stream().map(HashIterator::toLookup).toArray(IntToLongFunction[]::new);

    LongSupplier[] svs = IntStream.range(0, selectors.length)
                                     .mapToObj(x -> toValueSupplier(selectors[x], lookups[x], groupings == null))
                                     .toArray(LongSupplier[]::new);

    if (Iterables.all(source, s -> s instanceof SingleValued)) {
      if (selectors.length == 1) {
        return collector -> collector.collect(svs[0].getAsLong());
      }
      if (byRow) {
        if (groupings == null || groupings.length == 0) {
          return collector -> collector.collect(hash(svs));
        }
        long[] hashes = new long[selectors.length];   // not needed when groupings.length == 1
        return collector -> {
          for (int i = 0; i < selectors.length; i++) {
            hashes[i] = svs[i].getAsLong();
          }
          for (int[] grouping : groupings) {
            if (grouping.length == 0) {
              collector.collect(NULL_HASH);
            } else {
              long hash = svs[grouping[0]].getAsLong();
              for (int i = 1; i < grouping.length; i++) {
                hash = _hash(hash, svs[grouping[i]].getAsLong());
              }
              collector.collect(hash);
            }
          }
        };
      }
      return collector -> {
        for (LongSupplier supplier : svs) {
          collector.collect(supplier.getAsLong());
        }
      };
    }
    if (byRow) {
      if (groupings == null) {
        return colector -> colector.collect(hash(svs));
      }
      long[][] hashes = new long[selectors.length][];
      Supplier<long[][]> mvs = () -> {
        for (int i = 0; i < hashes.length; i++) {
          hashes[i] = toHashArray(selectors[i].getRow(), svs[i], lookups[i]);
        }
        return hashes;
      };
      if (groupings.length == 0) {
        return collector -> collectExploded(mvs.get(), collector);
      }
      return collector -> collectExploded(mvs.get(), collector, groupings);
    }
    return collector -> {
      for (int i = 0; i < selectors.length; i++) {
        collectAll(selectors[i].getRow(), svs[i], lookups[i], collector);
      }
    };
  }

  private static IntToLongFunction toLookup(DimensionSelector selector)
  {
    if (selector instanceof WithRawAccess) {
      return x -> Murmur3.hash64(((WithRawAccess) selector).getAsRef(x));
    } else {
      return x -> Murmur3.hash64(StringUtils.toUtf8WithNullToEmpty(selector.lookupName(x)));
    }
  }

  private static LongSupplier toValueSupplier(DimensionSelector selector, IntToLongFunction lookup, boolean concat)
  {
    if (concat) {
      return toConcatSupplier(selector, lookup);
    } else {
      return toFirstValueSupplier(selector, lookup);
    }
  }

  // called only when size == 1
  private static LongSupplier toFirstValueSupplier(DimensionSelector selector, IntToLongFunction lookup)
  {
    if (selector instanceof ObjectColumnSelector) {
      ValueDesc type = ((ObjectColumnSelector) selector).type();
      ValueType serializer = type.isDimension() || type.isMultiValued() ? ValueType.STRING : type.type();
      return () -> Murmur3.hash64(serializer.toBytes(((ObjectColumnSelector) selector).get()));
    }
    return () -> lookup.applyAsLong(selector.getRow().get(0));
  }

  private static LongSupplier toConcatSupplier(DimensionSelector selector, IntToLongFunction lookup)
  {
    if (selector instanceof ObjectColumnSelector || selector instanceof SingleValued) {
      return toFirstValueSupplier(selector, lookup);
    }
    return () -> {
      IndexedInts row = selector.getRow();
      int size = row.size();
      if (size == 0) {
        return NULL_HASH;
      }
      if (size == 1) {
        return lookup.applyAsLong(row.get(0));
      }
      int[] array = row.toArray();
      Arrays.sort(array);
      long hash = lookup.applyAsLong(array[0]);
      for (int x = 1; x < size; x++) {
        hash = _hash(hash, lookup.applyAsLong(array[x]));
      }
      return hash;
    };
  }

  private static long[] toHashArray(IndexedInts row, LongSupplier sv, IntToLongFunction lookup)
  {
    final int size = row.size();
    if (size == 0) {
      return new long[]{NULL_HASH};
    }
    if (size == 1) {
      return new long[]{sv.getAsLong()};
    }
    final long[] mvs = new long[size];
    for (int i = 0; i < mvs.length; i++) {
      mvs[i] = lookup.applyAsLong(row.get(i));
    }
    return mvs;
  }

  private static void collectAll(IndexedInts row, LongSupplier sv, IntToLongFunction lookup, HashCollector collector)
  {
    final int size = row.size();
    if (size == 0) {
      collector.collect(NULL_HASH);
    } else if (size == 1) {
      collector.collect(sv.getAsLong());
    } else {
      for (int i = 0; i < size; i++) {
        collector.collect(lookup.applyAsLong(row.get(i)));
      }
    }
  }

  private static void collectExploded(long[][] source, HashCollector collector)
  {
    collectExploded(collector, source, 0, 0);
  }

  // mimics group-by like population of multi-valued dimension
  private static void collectExploded(long[][] source, HashCollector collector, int[][] groupings)
  {
    for (int[] grouping : groupings) {
      long[][] copy = new long[source.length][];
      for (int index : grouping) {
        copy[index] = source[index];
      }
      collectExploded(collector, copy, 0, 0);
    }
  }

  private static void collectExploded(HashCollector collector, long[][] source, int index, long hash)
  {
    if (source.length == index) {
      collector.collect(hash);
      return;
    }
    if (source[index] == null || source[index].length == 0) {
      collectExploded(collector, source, index + 1, _hash(hash, NULL_HASH));
    } else if (source[index].length == 1) {
      collectExploded(collector, source, index + 1, _hash(hash, source[index][0]));
    } else {
      for (long element : source[index]) {
        collectExploded(collector, source, index + 1, _hash(hash, element));
      }
    }
  }

  private static long hash(LongSupplier[] suppliers)
  {
    long hash = suppliers[0].getAsLong();
    for (int i = 1; i < suppliers.length; i++) {
      hash = _hash(hash, suppliers[i].getAsLong());
    }
    return hash;
  }

  private static long _hash(long prev, long current)
  {
    return prev * 31 + current;
  }
}
