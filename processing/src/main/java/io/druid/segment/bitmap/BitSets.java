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

import java.util.Arrays;
import java.util.BitSet;
import java.util.function.IntFunction;

public class BitSets
{
  public static final int ADDRESS_BITS_PER_WORD = 6;
  public static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;

  public static <T> Iterable<T> transfrom(BitSet bitset, IntFunction<T> function)
  {
    return () -> IntIterators.transfrom(iterator(bitset), function);
  }

  public static IntIterator iterator(BitSet bitmap)
  {
    return new BatchIterator(bitmap);
  }

  public static BitSet flip(BitSet indices, int from, int to)
  {
    BitSet clone = (BitSet) indices.clone();
    clone.flip(from, to);
    return clone;
  }

  private static class BatchIterator implements IntIterator
  {
    private final long[] words;

    private int u;
    private final int[] batch;
    private int index;
    private int valid;

    private BatchIterator(BitSet bitmap)
    {
      words = bitmap.toLongArray();
      batch = new int[BITS_PER_WORD];
      advance(0);
    }

    private BatchIterator(long[] words, int u, int[] batch, int index, int valid)
    {
      this.words = Arrays.copyOf(words, words.length);
      this.u = u;
      this.batch = Arrays.copyOf(batch, batch.length);
      this.index = index;
      this.valid = valid;
    }

    private void advance(int wx)
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
        u = wx;
        break;
      }
    }

    @Override
    public boolean hasNext()
    {
      return index < valid;
    }

    @Override
    public int next()
    {
      if (index < valid) {
        final int next = batch[index++];
        if (index == valid) {
          advance(u + 1);
        }
        return next;
      }
      return -1;
    }

    @Override
    public IntIterator clone()
    {
      return new BatchIterator(words, u, batch, index, valid);
    }
  }
}
