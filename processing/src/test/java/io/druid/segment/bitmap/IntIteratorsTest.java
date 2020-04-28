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

import java.util.Arrays;

public class IntIteratorsTest
{
  @Test
  public void testBasic()
  {
    final int[] source = {1, 8, 3, 6, 5, 4, 7, 2, 9, 0};
    IntIterator basic = new IntIterators.FromArray(source);
    Assert.assertArrayEquals(source, IntIterators.toArray(basic));

    final int[] source0 = {};
    final int[] source1 = {1, 2, 7, 9};
    final int[] source2 = {1, 3, 7};
    final int[] source3 = {1, 2, 5, 7, 8, 9};
    final IntIterator sorted = new IntIterators.Sorted(Arrays.asList(
        new IntIterators.FromArray(source0),
        new IntIterators.FromArray(source1),
        new IntIterators.FromArray(source2),
        new IntIterators.FromArray(source3)
    ));
    Assert.assertArrayEquals(new int[]{1, 1, 1, 2, 2, 3, 5, 7, 7, 7, 8, 9, 9}, IntIterators.toArray(sorted));

    final IntIterator or = new IntIterators.OR(Arrays.asList(
        new IntIterators.FromArray(source0),
        new IntIterators.FromArray(source1),
        new IntIterators.FromArray(source2),
        new IntIterators.FromArray(source3)
    ));
    Assert.assertArrayEquals(new int[]{1, 2, 3, 5, 7, 8, 9}, IntIterators.toArray(or));

    final IntIterator and0 = new IntIterators.AND(Arrays.asList(
        new IntIterators.FromArray(source0),
        new IntIterators.FromArray(source2),
        new IntIterators.FromArray(source3)
    ));
    Assert.assertArrayEquals(new int[]{}, IntIterators.toArray(and0));

    final IntIterator and = new IntIterators.AND(Arrays.asList(
        new IntIterators.FromArray(source1),
        new IntIterators.FromArray(source2),
        new IntIterators.FromArray(source3)
    ));
    Assert.assertArrayEquals(new int[]{1, 7}, IntIterators.toArray(and));

    final IntIterators.NOT not0 = new IntIterators.NOT(new IntIterators.FromArray(source0), 10);
    final IntIterators.NOT not1 = new IntIterators.NOT(new IntIterators.FromArray(source1), 10);
    final IntIterators.NOT not2 = new IntIterators.NOT(new IntIterators.FromArray(source2), 10);
    final IntIterators.NOT not3 = new IntIterators.NOT(new IntIterators.FromArray(source3), 10);

    Assert.assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, IntIterators.toArray(not0));
    Assert.assertArrayEquals(new int[]{0, 3, 4, 5, 6, 8}, IntIterators.toArray(not1));
    Assert.assertArrayEquals(new int[]{0, 2, 4, 5, 6, 8, 9}, IntIterators.toArray(not2));
    Assert.assertArrayEquals(new int[]{0, 3, 4, 6}, IntIterators.toArray(not3));

    final IntIterator not_or = new IntIterators.NOT(new IntIterators.OR(Arrays.asList(
        new IntIterators.FromArray(source0),
        new IntIterators.FromArray(source1),
        new IntIterators.FromArray(source2),
        new IntIterators.FromArray(source3)
    )), 10);
    Assert.assertArrayEquals(new int[]{0, 4, 6}, IntIterators.toArray(not_or));

    final IntIterator not_and = new IntIterators.NOT(new IntIterators.AND(Arrays.asList(
        new IntIterators.FromArray(source1),
        new IntIterators.FromArray(source2),
        new IntIterators.FromArray(source3)
    )), 10);
    Assert.assertArrayEquals(new int[]{0, 2, 3, 4, 5, 6, 8, 9}, IntIterators.toArray(not_and));
  }
}