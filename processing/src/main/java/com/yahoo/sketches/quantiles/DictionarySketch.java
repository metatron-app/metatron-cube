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

package com.yahoo.sketches.quantiles;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.UTF8Bytes;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.bitmap.IntIterators;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

public class DictionarySketch
{
  public static final int DEFAULT_K = 8192;

  public static DictionarySketch newInstance()
  {
    return newInstance(DEFAULT_K);
  }

  public static DictionarySketch newInstance(int K)
  {
    return new DictionarySketch(K);
  }

  // no copy
  public static <T> ItemsUnion<T> toUnion(ItemsSketch<T> sketch)
  {
    ItemsUnion<T> union = ItemsUnion.getInstance(sketch.getK(), sketch.getComparator());
    union.gadget_ = sketch;
    return union;
  }

  // no copy
  public static <T> ItemsSketch<T> getResult(ItemsUnion<T> union)
  {
    return union.gadget_;
  }

  private final int K;
  private int[] sketch;
  private int index;
  private long N;
  private long bitPattern;
  private int min = -1;
  private int max = -1;

  private DictionarySketch(int k)
  {
    this.sketch = new int[k << 1];
    this.K = k;
  }

  private void maybeGrowLevels(long N)
  {
    final int numLevelsNeeded = Util.computeNumLevelsNeeded(K, N);
    if (numLevelsNeeded == 0) {
      // don't need any levels yet, and might have small base buffer; this can happen during a merge
      return;
    }
    // from here on we need a full-size base buffer and at least one level
    assert numLevelsNeeded > 0;
    final int spaceNeeded = offset(numLevelsNeeded);
    if (spaceNeeded > sketch.length) {
      // copies base buffer plus old levels
      sketch = Arrays.copyOf(sketch, spaceNeeded);
    }
  }

  private int offset(int level)
  {
    return (2 + level) * K;
  }

  public void update(int x)
  {
    sketch[index++] = x;
    min = min < 0 || min > x ? x : min;
    max = max < 0 || max < x ? x : max;
    N++;
    if (index == 2 * K) {
      Arrays.sort(sketch, 0, index);
      maybeGrowLevels(N);
      inPlacePropagateCarry();
      index = 0;
    }
  }

  private void inPlacePropagateCarry()
  {
    final int destLvl = Util.lowestZeroBitStartingAt(bitPattern, 0);
    final int destPos = offset(destLvl);

    zipSize2KBuffer(sketch, 0, sketch, destPos, K);

    for (int lvl = 0; lvl < destLvl; lvl++) {
      assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
      mergeTwoSizeKBuffers(sketch, offset(lvl), sketch, destPos, sketch, 0, K);
      zipSize2KBuffer(sketch, 0, sketch, destPos, K);
    } // end of loop over lower levels

    // update bit pattern with binary-arithmetic ripple carry
    bitPattern += 1L;
    assert N / (2L * K) == bitPattern;  // internal consistency check
  }

  public void merge(final DictionarySketch src)
  {
    Preconditions.checkArgument(src.K == K);

    for (int i = 0; i < src.index; i++) { //update only the base buffer
      update(src.sketch[i]);
    }
    maybeGrowLevels(N + src.N);

    assert src.bitPattern == (src.N / (2L * src.K));

    inPlacePropagateCarry(src);

    min = Math.min(min, src.min);
    max = Math.max(max, src.max);
    N = N + src.N;

    assert N / (2L * K) == bitPattern; // internal consistency check
  }

  private void inPlacePropagateCarry(final DictionarySketch src)
  {
    final int[] scratch = new int[2 * K];
    for (int srcLvl = 0; src.bitPattern != 0L; srcLvl++, src.bitPattern >>>= 1) {
      if ((src.bitPattern & 1L) > 0L) {   //only one level above base buffer
        final int destLvl = Util.lowestZeroBitStartingAt(bitPattern, srcLvl);
        final int destPos = offset(destLvl);
        System.arraycopy(src.sketch, src.offset(srcLvl), sketch, destPos, K); // src to dest

        for (int lvl = srcLvl; lvl < destLvl; lvl++) {
          assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
          mergeTwoSizeKBuffers(sketch, offset(lvl), sketch, destPos, scratch, 0, K); // merge & sort on scratch
          zipSize2KBuffer(scratch, 0, sketch, destPos, K); // scratch to dest
        }
        // update bit pattern with binary-arithmetic ripple carry
        bitPattern += 1L << srcLvl;
      }
    }
  }

  public ItemsSketch<UTF8Bytes> convert(final DimensionSelector selector)
  {
    ItemsSketch<UTF8Bytes> qs = ItemsSketch.getInstance(K, GuavaUtils.nullFirstNatural());
    qs.n_ = N;
    qs.combinedBufferItemCapacity_ = sketch.length;
    qs.combinedBuffer_ = new Object[sketch.length];
    qs.baseBufferCount_ = index;
    qs.bitPattern_ = bitPattern;

    Int2ObjectFunction<UTF8Bytes> handler =
        selector instanceof WithRawAccess ?
        x -> ((WithRawAccess) selector).getAsWrap(x) :
        x -> UTF8Bytes.of(StringUtils.toUtf8(Objects.toString(selector.lookupName(x), "")));

    for (int i = 0; i < index; i++) {
      qs.combinedBuffer_[i] = handler.apply(sketch[i]);
    }
    long current = bitPattern;
    for (int level = 0; current != 0L; level++, current >>>= 1) {
      if ((current & 1L) > 0L) {
        final int position = offset(level);
        final int limit = position + K;
        for (int i = position; i < limit; i++) {
          qs.combinedBuffer_[i] = handler.apply(sketch[i]);
        }
      }
    }

    qs.minValue_ = handler.apply(min);
    qs.maxValue_ = handler.apply(max);

    return qs;
  }

  //note: this version refers to the ItemsSketch.rand
  private static void zipSize2KBuffer(
      final int[] source, final int srcPos,
      final int[] dest, final int destPos,
      final int k
  )
  {
    final int limit = destPos + k;
    final int token = ItemsSketch.rand.nextBoolean() ? 1 : 0;
    for (int a = srcPos + token, c = destPos; c < limit; a += 2, c++) {
      dest[c] = source[a];
    }
  }

  private static <T> void mergeTwoSizeKBuffers(
      final int[] sketch1, final int offset1,
      final int[] sketch2, final int offset2,
      final int[] scratch, final int offset3,
      final int k
  )
  {
    final int arrStop1 = offset1 + k;
    final int arrStop2 = offset2 + k;

    int i1 = offset1;
    int i2 = offset2;
    int i3 = offset3;
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (sketch2[i2] < sketch1[i1]) {
        scratch[i3++] = sketch2[i2++];
      } else {
        scratch[i3++] = sketch1[i1++];
      }
    }

    if (i1 < arrStop1) {
      System.arraycopy(sketch1, i1, scratch, i3, arrStop1 - i1);
    } else {
      assert i2 < arrStop2;
      System.arraycopy(sketch2, i2, scratch, i3, arrStop2 - i2);
    }
  }


  // slower
  private void lookup(final DimensionSelector selector, final Object[] array)
  {
    final int level = Util.computeNumLevelsNeeded(K, N);
    final PriorityQueue<PeekingIterator<int[]>> queue = new ObjectHeapPriorityQueue<>(
        level + 1, (v1, v2) -> Integer.compare(v1.peek()[1], v2.peek()[1]));
    if (index > 0) {
      Arrays.sort(sketch, 0, index);
      PeekingIterator<int[]> iterator = wrap(0, IntIterators.from(sketch, 0, index));
      if (iterator.hasNext()) {
        queue.enqueue(iterator);
      }
    }
    for (int i = 0; i < level; i++) {
      if ((bitPattern & 1L << i) > 0) {
        int position = offset(i);
        PeekingIterator<int[]> iterator = wrap(position, IntIterators.from(sketch, position, position + K));
        if (iterator.hasNext()) {
          queue.enqueue(iterator);
        }
      }
    }
    while (!queue.isEmpty()) {
      final PeekingIterator<int[]> iterator = queue.first();
      final int[] x = iterator.next();
      array[x[0]++] = selector.lookupName(x[1]);
      if (iterator.hasNext()) {
        queue.changed();
      } else {
        queue.dequeue();
      }
    }
  }

  public static PeekingIterator<int[]> wrap(int position, IntIterator iterator)
  {
    return Iterators.peekingIterator(new Iterator<int[]>()
    {
      private final int[] x = {position, -1};

      @Override
      public boolean hasNext()
      {
        return iterator.hasNext();
      }

      @Override
      public int[] next()
      {
        x[1] = iterator.next();
        return x;
      }
    });
  }
}
