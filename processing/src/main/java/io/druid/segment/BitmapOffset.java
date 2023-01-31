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

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.data.Offset;
import io.druid.segment.filter.Filters;
import org.roaringbitmap.IntIterator;

/**
 */
public class BitmapOffset implements Offset
{
  private static final int INVALID_VALUE = -1;

  private final IntIterator itr;
  private final BitmapFactory bitmapFactory;
  private final ImmutableBitmap bitmapIndex;
  private final boolean descending;
  private final int numRows;

  private volatile int val;

  public BitmapOffset(BitmapFactory bitmapFactory, int numRows, ImmutableBitmap bitmapIndex, boolean descending)
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmapIndex = bitmapIndex;
    this.descending = descending;
    this.numRows = numRows;
    this.itr = Filters.newIterator(bitmapIndex, descending);
    increment();
  }

  private BitmapOffset(BitmapOffset otherOffset)
  {
    this.bitmapFactory = otherOffset.bitmapFactory;
    this.bitmapIndex = otherOffset.bitmapIndex;
    this.descending = otherOffset.descending;
    this.numRows = otherOffset.numRows;
    this.itr = otherOffset.itr.clone();
    this.val = otherOffset.val;
  }

  @Override
  public boolean increment()
  {
    if (itr.hasNext()) {
      val = itr.next();
      return val > INVALID_VALUE && val < numRows;
    } else {
      val = INVALID_VALUE;
      return false;
    }
  }

  @Override
  public int incrementN(int n)
  {
    for (; n > 0 && itr.hasNext(); n--) {
      val = itr.next();
    }
    return n;
  }

  @Override
  public boolean withinBounds()
  {
    return val > INVALID_VALUE && val < numRows;
  }

  @Override
  public Offset clone()
  {
    if (bitmapIndex == null || bitmapIndex.isEmpty()) {
      return new BitmapOffset(bitmapFactory, numRows, bitmapFactory.makeEmptyImmutableBitmap(), descending);
    }

    return new BitmapOffset(this);
  }

  @Override
  public int getOffset()
  {
    return val < numRows ? val : INVALID_VALUE;
  }
}
