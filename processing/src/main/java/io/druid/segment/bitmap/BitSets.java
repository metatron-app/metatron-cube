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
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.BitSet;
import java.util.function.IntFunction;

public class BitSets
{
  public static final int ADDRESS_BITS_PER_WORD = 6;
  public static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
  private final static int BIT_INDEX_MASK = BITS_PER_WORD - 1;

  public static int wordIndex(int bitIndex)
  {
    return bitIndex >> ADDRESS_BITS_PER_WORD;
  }

  public static int indexInWord(int bitIndex)
  {
    return bitIndex & BIT_INDEX_MASK;
  }

  public static long[] makeWords(int numRows)
  {
    return new long[wordIndex(numRows - 1) + 1];
  }

  public static <T> Iterable<T> transfrom(BitSet bitset, IntFunction<T> function)
  {
    return () -> IntIterators.transfrom(iterator(bitset), function);
  }

  public static IntIterator iterator(BitSet bitmap)
  {
    return bitmap == null ? null : iterator(bitmap, 0, bitmap.length());
  }

  public static IntIterator iterator(BitSet bitmap, int offset)
  {
    return bitmap == null ? null : iterator(bitmap, offset, bitmap.length());
  }

  public static IntIterator iterator(BitSet bitmap, int offset, int limit)
  {
    return bitmap == null ? null : new BatchIterator(bitmap, offset, limit);
  }

  public static long[] ensureCapacity(long[] prev, int capacity)
  {
    return prev.length < capacity ? new long[capacity] : prev;
  }

  public static BitSet collect(IntIterator iterator)
  {
    BitSet bits = new BitSet();
    while (iterator.hasNext()) {
      bits.set(iterator.next());
    }
    return bits;
  }

  public static BitSet flip(BitSet indices, int from, int to)
  {
    BitSet clone = (BitSet) indices.clone();
    clone.flip(from, to);
    return clone;
  }

  public static ImmutableBitmap wrap(BitSet bitSet)
  {
    return new WrappedBitSetBitmap(bitSet);
  }

  private static class BatchIterator implements IntIterator
  {
    private final long[] words;
    private final int limit;

    private final int[] batch;
    private int index;
    private int valid;

    private BatchIterator(BitSet bitmap, int offset, int limit)
    {
      this.words = bitmap.toLongArray();
      this.limit = limit;
      this.batch = new int[BITS_PER_WORD];
      int wx = wordIndex(offset);
      if (advance(wx) == wx && indexInWord(offset) > 0) {
        int ix = Arrays.binarySearch(batch, 0, valid, offset);
        index = ix < 0 ? -ix - 1 : ix;
      }
    }

    private BatchIterator(long[] words, int[] batch, int index, int valid, int limit)
    {
      this.words = Arrays.copyOf(words, words.length);
      this.batch = Arrays.copyOf(batch, batch.length);
      this.index = index;
      this.valid = valid;
      this.limit = limit;
    }

    private int advance(int wx)
    {
      index = valid = 0;
      for (; wx < words.length; wx++) {
        final long word = words[wx];
        if (word == 0) {
          continue;
        }
        int x = 0;
        int bx = Long.numberOfTrailingZeros(word);
        final int offset = wx * BITS_PER_WORD;
        for (long v = (word >>> bx); v != 0; v >>>= 1, bx++) {
          if ((v & 0x01) != 0) {
            batch[x++] = offset + bx;
          }
        }
        valid = x;
        break;
      }
      return wx;
    }

    @Override
    public boolean hasNext()
    {
      return index < valid && batch[index] < limit;
    }

    @Override
    public int next()
    {
      if (index < valid) {
        final int next = batch[index++];
        if (index == valid) {
          advance(wordIndex(next) + 1);
        }
        return next;
      }
      return -1;
    }

    @Override
    public IntIterator clone()
    {
      return new BatchIterator(words, batch, index, valid, limit);
    }
  }
}
