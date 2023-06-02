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

package io.druid.segment;

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.ordering.Direction;
import io.druid.segment.data.Offset;
import io.druid.segment.filter.Filters;
import org.roaringbitmap.IntIterator;

/**
 */
public final class BitmapOffset implements Offset
{
  private static final int INVALID_VALUE = -1;

  private IntIterator itr;
  private final ImmutableBitmap bitmap;
  private final Direction direction;
  private final int[] range;    // inclusive ~ inclusive

  private int val;  // volatile seemed overkill because cursor is single threaded

  public BitmapOffset(ImmutableBitmap bitmap, Direction direction, int[] range)
  {
    this.bitmap = bitmap;
    this.direction = direction;
    this.range = range;
    reset();
  }

  @Override
  public int get()
  {
    return val;
  }

  @Override
  public boolean increment()
  {
    if (itr.hasNext()) {
      return inBounds(val = itr.next());
    }
    val = INVALID_VALUE;
    return false;
  }

  @Override
  public int incrementN(int n)
  {
    for (; n > 0 && itr.hasNext(); n--) {
      if (!inBounds(val = itr.next())) {
        break;
      }
    }
    return n;
  }

  @Override
  public boolean withinBounds()
  {
    return inBounds(val);
  }

  @Override
  public Offset nextSpan()
  {
    if (direction == Direction.ASCENDING) {
      for (; val < range[0] && itr.hasNext(); val = itr.next()) {
      }
    } else {
      for (; val > range[1] && itr.hasNext(); val = itr.next()) {
      }
    }
    if (!withinBounds() && !itr.hasNext()) {
      val = INVALID_VALUE;
    }
    return this;
  }

  @Override
  public void reset()
  {
    this.itr = Filters.newIterator(bitmap, direction, direction == Direction.ASCENDING ? range[0] : range[1]);
    for (val = itr.next(); !withinBounds() && itr.hasNext(); val = itr.next()) {
    }
    if (!withinBounds() && !itr.hasNext()) {
      val = INVALID_VALUE;
    }
  }

  private boolean inBounds(int val)
  {
    return range[0] <= val && val <= range[1];
  }
}
