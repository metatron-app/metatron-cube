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

import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.BitSet;

public interface IntIterable
{
  IntIterator iterator();

  interface MaxAware extends IntIterable
  {
    int max();
  }

  class Range implements MaxAware
  {
    private final int from;
    private final int to;

    public Range(int from, int to)
    {
      this.from = from;
      this.to = to;
    }

    @Override
    public int max()
    {
      return to;
    }

    @Override
    public IntIterator iterator()
    {
      return new IntIterators.Range(from, to);
    }

    public boolean isEmpty()
    {
      return from > to;
    }

    public boolean get(int index)
    {
      return from <= index && index <= to;
    }

    public void or(BitSet bitSet)
    {
      bitSet.set(from, to + 1);   // exclusive
    }

    public void and(BitSet bitSet)
    {
      bitSet.clear(0, from);
      bitSet.clear(to + 1, Math.max(to + 1, bitSet.length()));
    }

    public void andNot(BitSet bitSet)
    {
      bitSet.clear(from, to + 1);
    }
  }

  class FromArray implements MaxAware
  {
    private final int[] array;

    public FromArray(int[] array) {this.array = array;}

    @Override
    public int max()
    {
      return array[array.length - 1];
    }

    @Override
    public IntIterator iterator()
    {
      return new IntIterators.FromArray(array);
    }

    public boolean get(int index)
    {
      return Arrays.binarySearch(array, index) >= 0;
    }
  }
}
