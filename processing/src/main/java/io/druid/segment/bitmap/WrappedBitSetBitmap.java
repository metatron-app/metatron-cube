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

public class WrappedBitSetBitmap extends com.metamx.collections.bitmap.WrappedBitSetBitmap
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

  public static IntIterator iterator(BitSet bitmap)
  {
    return new SimpleIterator(bitmap);
  }

  private static class SimpleIterator implements IntIterator
  {
    private final BitSet bitmap;
    private int next;

    private SimpleIterator(BitSet bitmap)
    {
      this(bitmap, bitmap.nextSetBit(0));
    }

    private SimpleIterator(BitSet bitmap, int next)
    {
      this.bitmap = bitmap;
      this.next = next;
    }

    @Override
    public boolean hasNext()
    {
      return next >= 0;
    }

    @Override
    public int next()
    {
      final int ret = next;
      next = bitmap.nextSetBit(next + 1);
      return ret;
    }

    @Override
    public IntIterator clone()
    {
      return new SimpleIterator(bitmap, next);
    }
  }
}
