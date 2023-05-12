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
import com.google.common.collect.Iterators;
import io.druid.collections.IntList;
import io.druid.common.Counter;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.dimension.DictionaryID;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.bitmap.IntIterators;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.commons.lang.mutable.MutableLong;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;

public class BatchGroupByQueryEngine
{
  private final GroupByQuery query;
  private final QueryConfig config;
  private final Cursor cursor;

  private static final long EOF = Long.MAX_VALUE;

  public BatchGroupByQueryEngine(GroupByQuery query, QueryConfig config, Cursor cursor)
  {
    this.query = query;
    this.config = config;
    this.cursor = cursor;
  }

  public Sequence<Object[]> process()
  {
    int batchSize = query.getContextInt(Query.GBY_BATCH_SIZE, -1);
    if (batchSize < 0 || query.getGroupings().length != 0) {
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
    final IntList[] batch = new IntList[Math.min(BLOCK_SIZE, batchSize)];
    for (int i = 0; i < batch.length; i++) {
      batch[i] = new IntList();
    }

    final int[] cardinalities = Arrays.stream(dimensions).mapToInt(DimensionSelector::getValueCardinality).toArray();

    final int[] bits = DictionaryID.bitsRequired(cardinalities);
    final int[] shifts = DictionaryID.bitsToShifts(bits);
    final int[] keyMask = DictionaryID.bitsToMasks(bits);

    final int keyBits = Arrays.stream(bits).sum();

    if (keyBits < Long.SIZE) {
      final Long2IntMap keyToBatch = new Long2IntOpenHashMap();
      final long[] batchToKey = new long[batch.length];
      final LongSupplier supplier = keys(dimensions, cardinalities, shifts);
      final Counter counter = new Counter();

      final Long fixedTimestamp = BaseQuery.getUniversalTimestamp(query, null);
      final int numColumns = dimensions.length + aggregators.length + 1;
      final Long timestamp = fixedTimestamp != null ? fixedTimestamp : cursor.getStartTime();

      final IntFunction[] rawAccess = DimensionSelector.convert(dimensions, dimensionTypes, useRawUTF8);

      return Sequences.withBaggage(Sequences.once(Iterators.concat(new Iterator<Iterator<Object[]>>()
      {
        @Override
        public boolean hasNext()
        {
          return !cursor.isDone();
        }

        @Override
        public Iterator<Object[]> next()
        {
          counter.reset();
          keyToBatch.clear();
          final int block = block(cursor.offset(), BLOCK_SIZE);
          for (; cursor.offset() < block && !cursor.isDone() && counter.intValue() < batch.length; cursor.advance()) {
            int batchId = keyToBatch.computeIfAbsent(
                supplier.getAsLong(), k -> {
                  int id = counter.getAndIncrement();
                  batchToKey[id] = k;
                  return id;
                }
            );
            batch[batchId].add(cursor.offset());
          }
          return new Iterator<Object[]>()
          {
            int ix;
            final int limit = counter.intValue();

            @Override
            public boolean hasNext()
            {
              return ix < limit;
            }

            @Override
            public Object[] next()
            {
              int i = 0;
              Object[] row = new Object[numColumns];
              row[i++] = new MutableLong(timestamp.longValue());
              final long key = batchToKey[ix];
              for (int x = 0; x < dimensions.length; x++) {
                row[i++] = rawAccess[x].apply(keyMask[x] & (int) (key >> shifts[x]));
              }
              for (int x = 0; x < aggregators.length; x++) {
                row[i++] = aggregators[x].aggregate(IntIterators.from(batch[ix]));
              }
              batch[ix++].clear();
              return row;
            }
          };
        }
      })), () -> Aggregators.close(aggregators));
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

  private static final int BLOCK_SIZE = 8192;

  private static int block(int current, int block)
  {
    final int num = current / block;
    return current % block == 0 ? current + block : (num + 1) * block;
  }
}
