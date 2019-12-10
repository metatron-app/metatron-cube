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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import java.nio.ShortBuffer;

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

  public static void addContainer(MutableRoaringArray array, int index, short key, IntArrayList values)
  {
    final MappeableContainer container;
    final IntListIterator iterator = values.iterator();
    if (values.size() < MappeableArrayContainer.DEFAULT_MAX_SIZE) {
      final ShortBuffer buffer = ShortBuffer.allocate(values.size());
      while (iterator.hasNext()) {
        buffer.put(lowbits(iterator.nextInt()));
      }
      container = new MappeableArrayContainer(buffer, values.size());
    } else {
      container = new MappeableBitmapContainer();
      while (iterator.hasNext()) {
        container.add(lowbits(iterator.nextInt()));
      }
    }
    array.insertNewKeyValueAt(index, key, container);
  }
}
