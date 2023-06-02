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

package io.druid.segment.data;

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.ordering.Direction;
import io.druid.segment.BitmapOffset;

/**
 * The "mutable" version of a ReadableOffset.  Introduces "increment()" and "withinBounds()" methods, which are
 * very similar to "next()" and "hasNext()" on the Iterator interface except increment() does not return a value.
 */
public interface Offset
{
  int get();

  boolean increment();

  default int incrementN(int n)
  {
    for (; n > 0 && increment(); n--) ;
    return n;
  }

  boolean withinBounds();

  Offset nextSpan();

  void reset();

  Offset EMPTY = new Offset()
  {
    @Override
    public int get() {return -1;}

    @Override
    public boolean increment() {return false;}

    @Override
    public boolean withinBounds() {return false;}

    @Override
    public Offset nextSpan()
    {
      return this;
    }

    @Override
    public void reset() {}
  };

  static Offset of(ImmutableBitmap bitmap, Direction direction, int[] range)
  {
    if (bitmap == null) {
      return new Range(direction, range);
    }
    if (bitmap.isEmpty()) {
      return EMPTY;
    }
    return new BitmapOffset(bitmap, direction, range);
  }

  final class Range implements Offset
  {
    private final int[] range;
    private final Direction direction;
    private int val;

    public Range(Direction direction, int[] range)
    {
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
      return inBounds(val = direction == Direction.ASCENDING ? val + 1 : val - 1);
    }

    @Override
    public boolean withinBounds()
    {
      return inBounds(val);
    }

    @Override
    public Offset nextSpan()
    {
      this.val = direction == Direction.ASCENDING ? range[0] : range[1];
      return this;
    }

    @Override
    public void reset()
    {
      this.val = direction == Direction.ASCENDING ? range[0] : range[1];
    }

    private boolean inBounds(int val)
    {
      return range[0] <= val && val <= range[1];
    }
  }
}
