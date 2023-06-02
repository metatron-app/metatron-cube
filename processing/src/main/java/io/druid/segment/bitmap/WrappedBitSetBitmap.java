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

import org.roaringbitmap.IntIterator;

import java.util.BitSet;

public class WrappedBitSetBitmap extends com.metamx.collections.bitmap.WrappedBitSetBitmap implements ExtendedBitmap
{
  public WrappedBitSetBitmap()
  {
    super();
  }

  public WrappedBitSetBitmap(BitSet bitSet)
  {
    super(bitSet);
  }

  public BitSet bitset()
  {
    return bitmap;
  }

  @Override
  public int first()
  {
    return bitmap.nextSetBit(0);
  }

  @Override
  public int last()
  {
    return bitmap.previousSetBit(bitmap.size());
  }

  @Override
  public IntIterator iterator(int offset)
  {
    return BitSets.iterator(bitmap, offset);
  }

  @Override
  public IntIterator iterator(int[] range)
  {
    if (range[0] > range[1]) {
      return IntIterators.EMPTY;
    }
    return BitSets.iterator(bitmap, range[0], range[1] + 1);
  }

  @Override
  public int cardinality(int[] range)
  {
    if (range[0] > range[1]) {
      return 0;
    }
    return bitmap.get(range[0], range[1]).cardinality();
  }
}
