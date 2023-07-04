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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.query.ordering.Direction;
import io.druid.segment.bitmap.BitSetBitmapFactory;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.List;

/**
 */
@Ignore("semantic changed")
@RunWith(Parameterized.class)
public class BitmapOffsetTest
{
  private static final int[] TEST_VALS = {1, 2, 4, 291, 27412, 49120, 212312, 2412101};
  private static final int[] TEST_VALS_FLIP = {2412101, 212312, 49120, 27412, 291, 4, 2, 1};

  @Parameterized.Parameters(name = "factory={0}, order={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return Iterables.transform(
        Sets.cartesianProduct(
            ImmutableSet.of(new ConciseBitmapFactory(), new RoaringBitmapFactory(), new BitSetBitmapFactory()),
            ImmutableSet.of(Direction.ASCENDING, Direction.DESCENDING)
        ),
        new Function<List<?>, Object[]>()
        {
          @Override
          public Object[] apply(List<?> input)
          {
            return input.toArray();
          }
        }
    );
  }

  private final BitmapFactory factory;
  private final Direction direction;

  public BitmapOffsetTest(BitmapFactory factory, Direction direction)
  {
    this.factory = factory;
    this.direction = direction;
  }

  @Test
  public void testSanity() throws Exception
  {
    MutableBitmap mutable = factory.makeEmptyMutableBitmap();
    for (int val : TEST_VALS) {
      mutable.add(val);
    }

    final BitmapOffset offset = new BitmapOffset(factory.makeImmutableBitmap(mutable), direction, new int[] {0, 2412101});
    final int[] expected = direction == Direction.ASCENDING ? TEST_VALS : TEST_VALS_FLIP;

    int count = 0;
    while (offset.withinBounds()) {
      Assert.assertEquals(expected[count], offset.get());

      int cloneCount = count;
      offset.reset();
      while (offset.withinBounds()) {
        Assert.assertEquals(expected[cloneCount], offset.get());

        ++cloneCount;
        offset.increment();
      }

      ++count;
      offset.increment();
    }
    Assert.assertEquals(expected.length, count);
  }
}
