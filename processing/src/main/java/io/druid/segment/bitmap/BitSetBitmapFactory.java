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

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;

public final class BitSetBitmapFactory implements BitmapFactory
{
  @Override
  public MutableBitmap makeEmptyMutableBitmap()
  {
    return new WrappedBitSetBitmap();
  }

  @Override
  public ImmutableBitmap makeEmptyImmutableBitmap()
  {
    return new WrappedBitSetBitmap();
  }

  @Override
  public ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap)
  {
    return mutableBitmap;
  }

  @Override
  public ImmutableBitmap mapImmutableBitmap(ByteBuffer b)
  {
    return new WrappedBitSetBitmap(BitSet.valueOf(b.array()));
  }

  @Override
  public ImmutableBitmap union(Iterable<ImmutableBitmap> b)
  {
    final BitSet bitSet = new BitSet();
    for (ImmutableBitmap bm : b) {
      bitSet.or(((WrappedBitSetBitmap) bm).bitset());
    }
    return new WrappedBitSetBitmap(bitSet);
  }

  @Override
  public ImmutableBitmap intersection(Iterable<ImmutableBitmap> b)
  {
    final Iterator<ImmutableBitmap> iterator = b.iterator();
    if (!iterator.hasNext()) {
      return makeEmptyImmutableBitmap();
    }
    final BitSet bitSet = (BitSet) ((WrappedBitSetBitmap) iterator.next()).bitset().clone();
    while (iterator.hasNext() && !bitSet.isEmpty()) {
      bitSet.and(((WrappedBitSetBitmap) iterator.next()).bitset());
    }
    return new WrappedBitSetBitmap(bitSet);
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b)
  {
    BitSet bitSet = (BitSet) ((WrappedBitSetBitmap) b).bitset().clone();
    bitSet.flip(0, bitSet.size());
    return new WrappedBitSetBitmap(bitSet);
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b, int length)
  {
    BitSet bitSet = (BitSet) ((WrappedBitSetBitmap) b).bitset().clone();
    bitSet.flip(0, length);
    return new WrappedBitSetBitmap(bitSet);
  }
}
