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

public class Bitmaps
{
  // first set bit
  public static int firstOf(ImmutableBitmap b, int defaultValue)
  {
    if (b == null || b.isEmpty()) {
      return defaultValue;
    } else if (b instanceof ExtendedBitmap) {
      return ((ExtendedBitmap) b).first();
    }
    return b.iterator().next();
  }

  // last set bit
  public static int lastOf(ImmutableBitmap b, int defaultValue)
  {
    if (b == null || b.isEmpty()) {
      return defaultValue;
    } else if (b instanceof ExtendedBitmap) {
      return ((ExtendedBitmap) b).last();
    }
    return defaultValue;
  }

  public static int[] range(ImmutableBitmap bitmap, int numRows)
  {
    return new int[]{firstOf(bitmap, 0), lastOf(bitmap, numRows - 1)};
  }

  public static void range(ImmutableBitmap bitmap, int numRows, int[] range)
  {
    range[0] = firstOf(bitmap, 0);
    range[1] = lastOf(bitmap, numRows - 1);
  }

  public static IntIterator filter(ImmutableBitmap bitmap, int[] range)
  {
    if (bitmap == null) {
      return IntIterators.fromTo(range[0], range[1] + 1);
    }
    if (bitmap.isEmpty()) {
      return IntIterators.EMPTY;
    }
    if (bitmap instanceof ExtendedBitmap) {
      return ((ExtendedBitmap) bitmap).iterator(range);
    }
    return IntIterators.and(bitmap.iterator(), IntIterators.fromTo(range[0], range[1] + 1));
  }

  public static int count(ImmutableBitmap bitmap, int[] range)
  {
    if (bitmap == null) {
      return range[1] - range[0] + 1;
    }
    if (bitmap.isEmpty() || range[0] > range[1]) {
      return 0;
    }
    if (bitmap instanceof ExtendedBitmap) {
      return ((ExtendedBitmap) bitmap).cardinality(range);
    }
    return IntIterators.count(filter(bitmap, range));
  }
}
