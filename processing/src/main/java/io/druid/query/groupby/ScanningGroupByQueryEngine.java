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

package io.druid.query.groupby;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.collections.IntList;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.BaseQuery;
import io.druid.query.QueryConfig;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.dimension.DictionaryID;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.bitmap.IntIterators;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.commons.lang.mutable.MutableLong;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

public class ScanningGroupByQueryEngine
{
  private final GroupByQuery query;
  private final QueryConfig config;
  private final Cursor cursor;

  private static final long EOF = Long.MAX_VALUE;

  public ScanningGroupByQueryEngine(GroupByQuery query, QueryConfig config, Cursor cursor)
  {
    this.query = query;
    this.config = config;
    this.cursor = cursor;
  }

  public Sequence<Object[]> process()
  {
    if (query.getGroupings().length != 0) {
      return null;
    }
    List<DimensionSpec> dimensionSpecs = query.getDimensions();
    DimensionSelector[] dimensions = new DimensionSelector[dimensionSpecs.size()];
    ValueDesc[] dimensionTypes = new ValueDesc[dimensionSpecs.size()];
    Supplier<TypeResolver> resolver = Suppliers.ofInstance(cursor);
    final boolean useRawUTF8 = config.useUTF8(query);
    for (int i = 0; i < dimensions.length; i++) {
      DimensionSpec dimensionSpec = dimensionSpecs.get(i);
      dimensions[i] = cursor.makeDimensionSelector(dimensionSpec);
      if (!(dimensions[i] instanceof DimensionSelector.Scannable)) {
        return null;  // should be single valued, dictionary encoded
      }
      dimensionTypes[i] = dimensionSpec.resolve(resolver);
    }
    Aggregator.Scannable[] aggregators = Aggregators.makeScannables(query.getAggregatorSpecs(), cursor);
    if (aggregators == null) {
      return null;
    }

    final int[] cardinalities = Arrays.stream(dimensions).mapToInt(DimensionSelector::getValueCardinality).toArray();

    final int[] bits = DictionaryID.bitsRequired(cardinalities);
    final int[] shifts = DictionaryID.bitsToShifts(bits);
    final int[] keyMask = DictionaryID.bitsToMasks(bits);

    final int keyBits = Arrays.stream(bits).sum();
    final int rowBits = DictionaryID.bitsRequired(cursor.size());
    final int rowMask = DictionaryID.bitToMask(rowBits);

    if (rowBits < Integer.SIZE && keyBits + rowBits < Long.SIZE) {
      final LongList list = new LongArrayList();
      final LongSupplier supplier = keys(dimensions, cardinalities, shifts);
      for (; !cursor.isDone(); cursor.advance()) {
        list.add((supplier.getAsLong() << rowBits) + cursor.offset());
      }
      list.add(EOF);

      final long[] keys = list.toLongArray();
      Arrays.sort(keys);

      final Long fixedTimestamp = BaseQuery.getUniversalTimestamp(query, null);
      final int numColumns = dimensions.length + aggregators.length + 1;
      final Long timestamp = fixedTimestamp != null ? fixedTimestamp : cursor.getStartTime();

      final IntFunction[] rawAccess = DimensionSelector.convert(dimensions, dimensionTypes, useRawUTF8);
      final LongFunction<Object[]> func = new LongFunction<Object[]>()
      {
        private long prev = -1;
        private final IntList collect = new IntList();

        @Override
        public Object[] apply(long value)
        {
          if (value == EOF) {
            return toRow(prev, collect);
          }
          final long key = value >> rowBits;
          final Object[] row = collect.isEmpty() || prev == key ? null : toRow(prev, collect);
          prev = key;
          collect.add((int) (value & rowMask));
          return row;
        }

        private Object[] toRow(long key, IntList collect)
        {
          int i = 0;
          Object[] row = new Object[numColumns];
          row[i++] = new MutableLong(timestamp.longValue());
          for (int x = 0; x < dimensions.length; x++) {
            row[i++] = rawAccess[x].apply(keyMask[x] & (int) (key >> shifts[x]));
          }
          for (Aggregator.Scannable scannable : aggregators) {
            row[i++] = scannable.aggregate(IntIterators.from(collect));
          }
          collect.clear();
          return row;
        }
      };
      return Sequences.once(Arrays.stream(keys).mapToObj(func).filter(v -> v != null).iterator());
    }
    return null;
  }

  private static LongSupplier keys(DimensionSelector[] selectors, int[] cardinalities, int[] shifts)
  {
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
}
