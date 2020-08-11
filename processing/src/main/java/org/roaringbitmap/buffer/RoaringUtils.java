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

import io.druid.segment.bitmap.WrappedBitSetBitmap;
import org.roaringbitmap.IntIterator;

import java.nio.ShortBuffer;
import java.util.BitSet;

// mostly for avoiding package local constrict of some methods
public class RoaringUtils
{
  public static short highbits(int x)
  {
    return (short) (x >>> 16);
  }

  public static short lowbits(int x)
  {
    return (short) (x & 0xFFFF);
  }

  public static int getNumContainers(ImmutableRoaringBitmap bitmap)
  {
    return bitmap.highLowContainer.size();
  }

  public static void addContainer(MutableRoaringArray array, short key, BitSet values)
  {
    addContainer(array, key, values, values.cardinality());
  }

  public static void addContainer(MutableRoaringArray array, short key, BitSet values, int cardinality)
  {
    array.append(key, container(values, cardinality));
  }

  private static MappeableContainer container(final BitSet bitSet, final int cardinality)
  {
    if (cardinality < MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      final ShortBuffer buffer = ShortBuffer.allocate(cardinality);
      final IntIterator iterator = WrappedBitSetBitmap.iterator(bitSet);
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
}
