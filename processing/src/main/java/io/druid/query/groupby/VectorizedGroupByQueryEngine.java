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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.collections.BufferPool;
import io.druid.collections.ResourceHolder;
import io.druid.common.guava.Iterators;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Row;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.aggregation.Aggregator.Vectorized;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.dimension.DictionaryID;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.Scannable;
import io.druid.segment.Segment;
import io.druid.segment.bitmap.IntIterable;
import io.druid.segment.bitmap.IntIterators;
import io.druid.utils.HeapSort;
import it.unimi.dsi.fastutil.ints.Int2LongFunction;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.lang.mutable.MutableLong;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;

public class VectorizedGroupByQueryEngine
{
  private static final Logger LOG = new Logger(VectorizedGroupByQueryEngine.class);

  private final BufferPool bufferPool;

  @Inject
  public VectorizedGroupByQueryEngine(@Global BufferPool bufferPool)
  {
    this.bufferPool = bufferPool;
  }

  public Sequence<Row> process(
      GroupByQuery query,
      QueryConfig config,
      Segment segment,
      boolean compact,
      SessionCache cache
  )
  {
    return QueryRunnerHelper.makeCursorBasedQueryConcat(
        segment, query, cache, cursor -> process(cursor, query, config)
    );
  }

  @SuppressWarnings("unchecked")
  private Sequence<Row> process(Cursor cursor, GroupByQuery query, QueryConfig config)
  {
    final int size = cursor.size();
    final List<DimensionSpec> dimensionSpecs = query.getDimensions();
    final Scannable[] dimensions = new Scannable[dimensionSpecs.size()];
    for (int i = 0; i < dimensions.length; i++) {
      dimensions[i] = (Scannable) cursor.makeDimensionSelector(dimensionSpecs.get(i));
    }
    final IntFunction[] rawAccess = DimensionSelector.convert(dimensions, null, config.useUTF8(query));

    final Vectorized[] aggregators = Aggregators.vectorize(query.getAggregatorSpecs(), cursor);
    Preconditions.checkArgument(aggregators != null);     // todo

    final int[] cardinalities = Arrays.stream(dimensions).mapToInt(DimensionSelector::getValueCardinality).toArray();

    // 1M : 20 bit, 10M : 24 bit
    final int[] bits = DictionaryID.bitsRequired(cardinalities);
    final int[] shifts = DictionaryID.bitsToShifts(bits);
    final int[] keyMask = DictionaryID.bitsToMasks(bits);
    final int bitsNeeded = Arrays.stream(bits).sum();

    // apply unsigned long ?
    Preconditions.checkArgument(bitsNeeded < Long.SIZE - 1, Arrays.toString(bits));    // todo

    final Long2IntOpenHashMap keyToBatch = new Long2IntOpenHashMap();
    final Object[] aggregations = new Object[aggregators.length];

    try (ResourceHolder<ByteBuffer> holder = bufferPool.take()) {
      final ByteBuffer buffer = holder.get();
      final IntIterable iterable = IntIterable.wrap(cursor);

      final IntBuffer ordering = buffer.asIntBuffer();    // row to batch
      final Int2LongFunction keyMaker = keys(iterable, buffer.asLongBuffer(), dimensions, bits);
      for (IntIterator iterator = IntIterators.from(iterable, size); iterator.hasNext(); ) {
        final int row = iterator.next();
        final long key = keyMaker.applyAsLong(row);
        ordering.put(row, keyToBatch.computeIfAbsent(key, k -> keyToBatch.size()));
      }
      for (int i = 0; i < aggregators.length; i++) {
        aggregations[i] = aggregators[i].init(keyToBatch.size());
      }
      for (int i = 0; i < aggregators.length; i++) {
        aggregators[i].aggregate(iterable.iterator(), aggregations[i], x -> ordering.get(x), size);
      }
    }
    final int limit = keyToBatch.size();
    final int numColumns = rawAccess.length + aggregators.length + 1;
    final long timestamp = BaseQuery.getUniversalTimestamp(query, cursor.getStartTime());

    if (bitsNeeded + DictionaryID.bitsRequired(limit) < Long.SIZE - 1) {
      final int shift = DictionaryID.bitsRequired(limit);
      final int mask = DictionaryID.bitToMask(shift);
      final long[] kv = new long[limit];
      final ObjectIterator<Long2IntMap.Entry> iterator = keyToBatch.long2IntEntrySet().fastIterator();
      for (int i = 0; iterator.hasNext(); i++) {
        Long2IntMap.Entry entry = iterator.next();
        kv[i] = (entry.getLongKey() << shift) + entry.getIntValue();
      }
      Arrays.sort(kv);

      return Sequences.once(Iterators.batch(Row.class, new Iterator<Row>()
      {
        private int ix;

        @Override
        public boolean hasNext()
        {
          return ix < limit;
        }

        @Override
        public Row next()
        {
          final int c = ix++;
          int i = 0;
          Object[] row = new Object[numColumns];
          row[i++] = new MutableLong(timestamp);
          final long k = kv[c] >> shift;
          for (int x = 0; x < rawAccess.length; x++) {
            row[i++] = rawAccess[x].apply(keyMask[x] & (int) (k >> shifts[x]));
          }
          for (int x = 0; x < aggregators.length; x++) {
            row[i++] = aggregators[x].get(aggregations[x], (int) (kv[c] & mask));
          }
          return new CompactRow(row);
        }
      }));
    }

    final long[] keys = new long[limit];
    final int[] values = new int[limit];
    final ObjectIterator<Long2IntMap.Entry> iterator = keyToBatch.long2IntEntrySet().fastIterator();
    for (int i = 0; iterator.hasNext(); i++) {
      Long2IntMap.Entry entry = iterator.next();
      keys[i] = entry.getLongKey();
      values[i] = entry.getIntValue();
    }
    HeapSort.sort(keys, values, 0, keys.length);

    return Sequences.once(Iterators.batch(Row.class, new Iterator<Row>()
    {
      private int ix;

      @Override
      public boolean hasNext()
      {
        return ix < limit;
      }

      @Override
      public Row next()
      {
        final int c = ix++;
        int i = 0;
        Object[] row = new Object[numColumns];
        row[i++] = new MutableLong(timestamp);
        for (int x = 0; x < rawAccess.length; x++) {
          row[i++] = rawAccess[x].apply(keyMask[x] & (int) (keys[c] >> shifts[x]));
        }
        for (int x = 0; x < aggregators.length; x++) {
          row[i++] = aggregators[x].get(aggregations[x], values[c]);
        }
        return new CompactRow(row);
      }
    }));
  }

  private static Int2LongFunction keys(IntIterable rows, LongBuffer keys, Scannable[] dimensions, int[] bits)
  {
    dimensions[0].consume(rows.iterator(), (x, v) -> keys.put(x, v));
    for (int i = 1; i < dimensions.length; i++) {
      int shift = bits[i];
      dimensions[i].consume(rows.iterator(), (x, v) -> keys.put(x, (keys.get(x) << shift) + v));
    }
    return x -> keys.get(x);
  }
}
