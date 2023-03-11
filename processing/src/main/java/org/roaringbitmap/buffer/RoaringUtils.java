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

package org.roaringbitmap.buffer;

import io.druid.segment.bitmap.BitSets;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.IntIterator;

import java.nio.CharBuffer;
import java.util.BitSet;

// mostly for avoiding package local constrict of some methods
public class RoaringUtils
{
  // from org.roaringbitmap.buffer.BufferUtil
  public static char highbits(int x)
  {
    return (char) (x >>> 16);
  }

  // from org.roaringbitmap.buffer.BufferUtil
  public static char lowbits(int x)
  {
    return (char) x;
  }

  public static int getNumContainers(ImmutableRoaringBitmap bitmap)
  {
    return bitmap.highLowContainer.size();
  }

  public static MappeableContainerPointer getContainerPointer(ImmutableRoaringBitmap bitmap)
  {
    return bitmap.highLowContainer.getContainerPointer();
  }

  public static void addContainer(MutableRoaringArray array, char key, BitSet values)
  {
    addContainer(array, key, values, values.cardinality());
  }

  public static void addContainer(MutableRoaringArray array, char key, BitSet values, int cardinality)
  {
    array.append(key, container(values, cardinality));
  }

  private static MappeableContainer container(final BitSet bitSet, final int cardinality)
  {
    if (cardinality < MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      final CharBuffer buffer = CharBuffer.allocate(cardinality);
      final IntIterator iterator = BitSets.iterator(bitSet);
      while (iterator.hasNext()) {
        buffer.put(lowbits(iterator.next()));
      }
      return new MappeableArrayContainer(buffer, cardinality);
    } else {
      final MappeableBitmapContainer container = new MappeableBitmapContainer();
      container.bitmap.put(bitSet.toLongArray());
      container.cardinality = cardinality;
      return container;
    }
  }

  public static void lazyor(MutableRoaringBitmap answer, ImmutableRoaringBitmap bitmap)
  {
    answer.naivelazyor(bitmap);
  }

  public static MutableRoaringBitmap repair(MutableRoaringBitmap answer)
  {
    answer.repairAfterLazy();
    return answer;
  }

  public static BatchIterator getBatchIterator(ImmutableRoaringBitmap bitmap)
  {
    return new RoaringBatchIterator(null == bitmap.highLowContainer ? null : bitmap.getContainerPointer());
  }
}
