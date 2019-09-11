/*
 * Copyright 2011 - 2015 SK Telecom Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.bitmap;

import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.roaringbitmap.IntIterator;

import java.util.BitSet;
import java.util.Iterator;

public final class RoaringBitmapFactory extends com.metamx.collections.bitmap.RoaringBitmapFactory
{
  public RoaringBitmapFactory()
  {
    super();
  }

  public RoaringBitmapFactory(boolean compressRunOnSerialization)
  {
    super(compressRunOnSerialization);
  }

  @Override
  public ImmutableBitmap makeEmptyImmutableBitmap()
  {
    return new WrappedImmutableRoaringBitmap(((WrappedImmutableRoaringBitmap) super.makeEmptyImmutableBitmap()).getBitmap())
    {
      @Override
      public boolean get(int value)
      {
        return false;
      }
    };
  }

  @Override
  public ImmutableBitmap union(Iterable<ImmutableBitmap> bitmaps)
  {
    final BitSet bitSet = new BitSet();
    for (ImmutableBitmap bitmap : bitmaps) {
      copyTo(bitmap, bitSet);
    }
    return finalize(bitSet);
  }

  @Override
  public ImmutableBitmap intersection(Iterable<ImmutableBitmap> bitmaps)
  {
    final Iterator<ImmutableBitmap> iterator = bitmaps.iterator();
    if (!iterator.hasNext()) {
      return makeEmptyImmutableBitmap();
    }
    final BitSet bitSet = copyTo(iterator.next(), new BitSet());
    while (iterator.hasNext() && !bitSet.isEmpty()) {
      final ImmutableBitmap bitmap = iterator.next();
      if (bitmap.isEmpty()) {
        return makeEmptyImmutableBitmap();
      }
      int last = -1;
      int next = bitSet.nextSetBit(0);
      final IntIterator values = bitmap.iterator();
      while (values.hasNext() && next >= 0) {
        final int x = values.next();
        if (x > next) {
          bitSet.clear(next, x);
          next = bitSet.nextSetBit(x + 1);
        } else if (x == next) {
          next = bitSet.nextSetBit(x + 1);
        }
        last = x;
      }
      if (bitSet.get(last)) {
        last++;
      }
      if (last < bitSet.size()) {
        bitSet.clear(last, bitSet.size());
      }
    }
    return finalize(bitSet);
  }

  private BitSet copyTo(final ImmutableBitmap bitmap, final BitSet bitSet)
  {
    final IntIterator iterator = bitmap.iterator();
    while (iterator.hasNext()) {
      bitSet.set(iterator.next());
    }
    return bitSet;
  }

  private ImmutableBitmap finalize(final BitSet bitSet)
  {
    if (bitSet == null || bitSet.isEmpty()) {
      return makeEmptyImmutableBitmap();
    }
    final MutableBitmap mutable = makeEmptyMutableBitmap();
    for (int x = bitSet.nextSetBit(0); x >= 0; x = bitSet.nextSetBit(x + 1)) {
      mutable.add(x);
    }
    return makeImmutableBitmap(mutable);
  }
}
