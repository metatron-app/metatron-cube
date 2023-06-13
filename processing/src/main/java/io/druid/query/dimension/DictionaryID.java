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

package io.druid.query.dimension;

import io.druid.common.guava.Sequence;
import io.druid.common.utils.Murmur3;
import io.druid.common.utils.Sequences;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector.Scannable;
import io.druid.segment.ScanContext;
import io.druid.segment.Scanning;
import io.druid.segment.bitmap.BitSets;
import io.druid.segment.bitmap.IntIterators;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.LongStream;

public interface DictionaryID
{
  static int[] bitsRequired(int[] cardinalities)
  {
    final int[] bits = new int[cardinalities.length];
    for (int i = 0; i < cardinalities.length; i++) {
      if (cardinalities[i] < 0) {
        return null;
      }
      bits[i] = bitsRequired(cardinalities[i]);
    }
    return bits;
  }

  static int[] bitsToShifts(int[] bits)
  {
    final int[] shifts = new int[bits.length];
    for (int i = bits.length - 2; i >= 0; i--) {
      shifts[i] = shifts[i + 1] + bits[i + 1];
    }
    return shifts;
  }

  static int[] bitsToMasks(int[] bits)
  {
    return Arrays.stream(bits).map(DictionaryID::bitToMask).toArray();
  }

  static int bitToMask(int bit)
  {
    return (1 << bit) - 1;
  }

  static int bitsRequired(int cardinality)
  {
    if (cardinality == 1) {
      return 1;
    }
    final double v = Math.log(cardinality) / Math.log(2);
    return v == (int) v ? (int) v : (int) v + 1;
  }

  // do things inside of cursors
  static LongStream collect(Sequence<Cursor> cursors, List<DimensionSpec> dimensions)
  {
    return Sequences.only(Sequences.map(cursors, cursor -> {

      ScanContext context = cursor.scanContext();
      Scannable scannable = (Scannable) cursor.makeDimensionSelector(dimensions.get(0));
      long[] dictHash0 = new long[scannable.getValueCardinality()];

      BitSet rowIds;
      BitSet dictId = new BitSet();
      if (context.is(Scanning.FULL)) {
        rowIds = null;
        scannable.getDictionary().scan(null, (x, b, o, l) -> dictHash0[x] = Murmur3.hash64(b, o, l));
      } else {
        if (context.awareTargetRows()) {
          rowIds = null;
          scannable.consume(context.iterator(), (x, v) -> dictId.set(v));
        } else {
          rowIds = new BitSet(context.count());
          scannable.consume(IntIterators.wrap(cursor), (x, v) -> {rowIds.set(x);dictId.set(v);});
        }
        scannable.getDictionary().scan(BitSets.iterator(dictId), (x, b, o, l) -> dictHash0[x] = Murmur3.hash64(b, o, l));
      }
      Hashes hash = new Hashes(cursor.size());
      scannable.consume(BitSets.iterator(rowIds), (x, v) -> hash.add(dictHash0[v]));

      long[] prev = dictHash0;
      for (int i = 1; i < dimensions.size(); i++, hash.next()) {
        scannable = (Scannable) cursor.makeDimensionSelector(dimensions.get(i));
        long[] dictHash = BitSets.ensureCapacity(prev, scannable.getValueCardinality());
        if (context.is(Scanning.FULL)) {
          scannable.getDictionary().scan(null, (x, b, o, l) -> dictHash[x] = Murmur3.hash64(b, o, l));
        } else {
          dictId.clear();
          IntIterator iterator = context.awareTargetRows() ? context.iterator() : BitSets.iterator(rowIds);
          scannable.consume(iterator, (x, v) -> dictId.set(v));
          scannable.getDictionary().scan(BitSets.iterator(dictId), (x, b, o, l) -> dictHash[x] = Murmur3.hash64(b, o, l));
        }
        scannable.consume(BitSets.iterator(rowIds), (x, v) -> hash.append(dictHash[v]));
        prev = dictHash;
      }
      return hash.toStream();
    }));
  }

  static class Hashes extends LongArrayList
  {
    private int offset;

    private Hashes(int size)
    {
      super(size);
    }

    private LongStream toStream()
    {
      return Arrays.stream(a, 0, size).filter(v -> v != 0);
    }

    private void next()
    {
      offset = 0;
    }

    private void append(long hash)
    {
      a[offset] = a[offset] * 31 + hash;
      offset++;
    }
  }
}
