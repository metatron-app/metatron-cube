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

package io.druid.segment.bitmap;

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.Cursor;
import io.druid.segment.ScanContext;
import io.druid.segment.Scanning;
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;

public interface IntIterable
{
  IntIterator iterator();

  interface MinMaxAware extends IntIterable
  {
    int min();

    int max();
  }

  IntIterable EMPTY = () -> IntIterators.EMPTY;

  // inclusive ~ exclusive
  class Range implements MinMaxAware
  {
    private final int from;
    private final int to;

    public Range(int from, int to)
    {
      this.from = from;
      this.to = to;
    }

    @Override
    public int min()
    {
      return from;
    }

    @Override
    public int max()
    {
      return to - 1;
    }

    @Override
    public IntIterator iterator()
    {
      return new IntIterators.Range(from, to);
    }

    public boolean get(int index)
    {
      return from <= index && index < to;
    }

    public void or(BitSet bitSet)
    {
      bitSet.set(from, to);
    }

    public void and(BitSet bitSet)
    {
      bitSet.clear(0, from);
      bitSet.clear(to, Math.max(to, bitSet.length()));
    }

    public void andNot(BitSet bitSet)
    {
      bitSet.clear(from, to);
    }
  }

  class FromArray implements MinMaxAware
  {
    private final int[] array;

    public FromArray(int[] array) {this.array = array;}

    @Override
    public int min()
    {
      return array[0];
    }

    @Override
    public int max()
    {
      return array[array.length - 1];
    }

    @Override
    public IntIterator iterator()
    {
      return IntIterators.from(array);
    }

    public boolean get(int index)
    {
      return Arrays.binarySearch(array, index) >= 0;
    }
  }

  static IntIterable wrap(Cursor cursor)
  {
    if (cursor.isDone()) {
      return EMPTY;
    }
    ScanContext context = cursor.scanContext();
    if (context.is(Scanning.FULL)) {
      return () -> null;
    }
    if (context.awareTargetRows()) {
      AtomicBoolean first = new AtomicBoolean(true);
      return () -> {
        if (!first.compareAndSet(true, false)) {
          cursor.reset();
        }
        return IntIterators.wrap(cursor);
      };
    }
    final BitSet rows = new BitSet();
    for (; !cursor.isDone(); cursor.advance()) {
      rows.set(cursor.offset());
    }
    return () -> BitSets.iterator(rows);
  }

  static IntIterable except(IntIterable cursor, ImmutableBitmap skip, int size)
  {
    return () -> IntIterators.except(cursor.iterator(), skip, size);
  }

  static IntIterable include(IntIterable cursor, ImmutableBitmap include)
  {
    return () -> IntIterators.include(cursor.iterator(), include);
  }
}
