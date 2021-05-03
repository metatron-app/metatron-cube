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

import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.IntIterator;

public class IntIteratorsTest
{
  private final int[] source = {1, 8, 3, 6, 5, 4, 7, 2, 9, 0};
  private final int[] source0 = {};
  private final int[] source1 = {1, 2, 7, 9};
  private final int[] source2 = {1, 3, 7};
  private final int[] source3 = {1, 2, 5, 7, 8, 9};

  @Test
  public void testBasic()
  {
    Assert.assertArrayEquals(source, IntIterators.toArray(IntIterators.from(source)));

    final IntIterator sorted = IntIterators.sort(
        IntIterators.from(source0),
        IntIterators.from(source1),
        IntIterators.from(source2),
        IntIterators.from(source3)
    );
    Assert.assertArrayEquals(new int[]{1, 1, 1, 2, 2, 3, 5, 7, 7, 7, 8, 9, 9}, IntIterators.toArray(sorted));

    Assert.assertArrayEquals(source0, IntIterators.toArray(IntIterators.or(IntIterators.from(source0))));
    Assert.assertArrayEquals(source1, IntIterators.toArray(IntIterators.or(IntIterators.from(source1))));
    Assert.assertArrayEquals(source2, IntIterators.toArray(IntIterators.or(IntIterators.from(source2))));
    Assert.assertArrayEquals(source3, IntIterators.toArray(IntIterators.or(IntIterators.from(source3))));

    final IntIterator or = IntIterators.or(
        IntIterators.from(source0),
        IntIterators.from(source1),
        IntIterators.from(source2),
        IntIterators.from(source3)
    );
    Assert.assertArrayEquals(new int[]{1, 2, 3, 5, 7, 8, 9}, IntIterators.toArray(or));

    Assert.assertArrayEquals(source0, IntIterators.toArray(IntIterators.and(IntIterators.from(source0))));
    Assert.assertArrayEquals(source1, IntIterators.toArray(IntIterators.and(IntIterators.from(source1))));
    Assert.assertArrayEquals(source2, IntIterators.toArray(IntIterators.and(IntIterators.from(source2))));
    Assert.assertArrayEquals(source3, IntIterators.toArray(IntIterators.and(IntIterators.from(source3))));

    final IntIterator and0 = IntIterators.and(
        IntIterators.from(source0),
        IntIterators.from(source2),
        IntIterators.from(source3)
    );
    Assert.assertArrayEquals(new int[]{}, IntIterators.toArray(and0));

    final IntIterator and = IntIterators.and(
        IntIterators.from(source1),
        IntIterators.from(source2),
        IntIterators.from(source3)
    );
    Assert.assertArrayEquals(new int[]{1, 7}, IntIterators.toArray(and));

    final IntIterator not0 = IntIterators.not(IntIterators.from(source0), 10);
    final IntIterator not1 = IntIterators.not(IntIterators.from(source1), 10);
    final IntIterator not2 = IntIterators.not(IntIterators.from(source2), 10);
    final IntIterator not3 = IntIterators.not(IntIterators.from(source3), 10);

    Assert.assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, IntIterators.toArray(not0));
    Assert.assertArrayEquals(new int[]{0, 3, 4, 5, 6, 8}, IntIterators.toArray(not1));
    Assert.assertArrayEquals(new int[]{0, 2, 4, 5, 6, 8, 9}, IntIterators.toArray(not2));
    Assert.assertArrayEquals(new int[]{0, 3, 4, 6}, IntIterators.toArray(not3));

    final IntIterator not_or = IntIterators.not(IntIterators.or(
        IntIterators.from(source0),
        IntIterators.from(source1),
        IntIterators.from(source2),
        IntIterators.from(source3)
    ), 10);
    Assert.assertArrayEquals(new int[]{0, 4, 6}, IntIterators.toArray(not_or));

    final IntIterator not_and = IntIterators.not(IntIterators.and(
        IntIterators.from(source1),
        IntIterators.from(source2),
        IntIterators.from(source3)
    ), 10);
    Assert.assertArrayEquals(new int[]{0, 2, 3, 4, 5, 6, 8, 9}, IntIterators.toArray(not_and));
  }

  @Test
  public void testDiff()
  {
    Assert.assertArrayEquals(source1, IntIterators.toArray(
        IntIterators.diff(IntIterators.from(source1), IntIterators.from(source0))
    ));
    Assert.assertArrayEquals(new int[0], IntIterators.toArray(
        IntIterators.diff(IntIterators.from(source1), IntIterators.from(source1))
    ));
    Assert.assertArrayEquals(new int[0], IntIterators.toArray(
        IntIterators.diff(IntIterators.from(source0), IntIterators.from(source1))
    ));

    // A and not B
    Assert.assertArrayEquals(new int[]{2, 5, 8, 9}, IntIterators.toArray(
        IntIterators.and(IntIterators.from(source3), IntIterators.not(IntIterators.from(source2), 10))
    ));
    Assert.assertArrayEquals(new int[]{2, 5, 8, 9}, IntIterators.toArray(
        IntIterators.diff(IntIterators.from(source3), IntIterators.from(source2))
    ));
  }
}
