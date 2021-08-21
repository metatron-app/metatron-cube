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
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.segment.bitmap.BitSetBitmapFactory;
import io.druid.segment.bitmap.IntIterable;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.Random;

public class BitmapTest
{
  @Test
  public void testOr()
  {
    int range = 20_0000;
    Random r = new Random();
    BitmapFactory f = new BitSetBitmapFactory();
    ImmutableBitmap[] bitmaps = new ImmutableBitmap[1_0000];
    for (int i = 0; i < bitmaps.length; i++) {
      int c = r.nextInt(400) + 100;
      MutableBitmap mutable = f.makeEmptyMutableBitmap();
      for (int j = 0; j < c; j++) {
        mutable.add(r.nextInt(range));
      }
      bitmaps[i] = f.makeImmutableBitmap(mutable);
    }
    long t = System.currentTimeMillis();
    ImmutableBitmap u = f.union(Arrays.asList(bitmaps));
    System.out.println("bitset(mod) took.. " + (System.currentTimeMillis() - t) + " msec");
    int size = u.size();

    f = new com.metamx.collections.bitmap.BitSetBitmapFactory();
    t = System.currentTimeMillis();
    f.union(Arrays.asList(bitmaps));  // invalid result..
    System.out.println("bitset(org) took.. " + (System.currentTimeMillis() - t) + " msec");

    f = new RoaringBitmapFactory();
    for (int i = 0; i < bitmaps.length; i++) {
      MutableBitmap mutable = f.makeEmptyMutableBitmap();
      IntIterator iterators = bitmaps[i].iterator();
      while (iterators.hasNext()) {
        mutable.add(iterators.next());
      }
      bitmaps[i] = f.makeImmutableBitmap(mutable);
    }
    t = System.currentTimeMillis();
    u = f.union(Arrays.asList(bitmaps));
    System.out.println("roaring(mod) took.. " + (System.currentTimeMillis() - t) + " msec");
    Assert.assertEquals(size, u.size());

    f = new com.metamx.collections.bitmap.RoaringBitmapFactory();
    t = System.currentTimeMillis();
    u = f.union(Arrays.asList(bitmaps));
    System.out.println("roaring(org) took.. " + (System.currentTimeMillis() - t) + " msec");
    Assert.assertEquals(size, u.size());
  }

  @Test
  public void testAnd()
  {
    int range = 20_0000;
    BitmapFactory f = new BitSetBitmapFactory();
    ImmutableBitmap[] bitmaps = new ImmutableBitmap[1_000];
    for (int i = 0; i < bitmaps.length; i++) {
      Random r = new Random(0);
      for (int j = 0; j < new Random().nextInt(10 * (i + 1)); j++) {
        r.nextInt();
      }
      int c = r.nextInt(2500) + 500;
      MutableBitmap mutable = f.makeEmptyMutableBitmap();
      for (int j = 0; j < c; j++) {
        mutable.add(r.nextInt(range));
      }
      bitmaps[i] = f.makeImmutableBitmap(mutable);
    }
    long t = System.currentTimeMillis();
    ImmutableBitmap u = f.intersection(Arrays.asList(bitmaps));
    System.out.println("bitset(mod) took.. " + (System.currentTimeMillis() - t) + " msec");
    int size = u.size();

    f = new com.metamx.collections.bitmap.BitSetBitmapFactory();
    t = System.currentTimeMillis();
    u = f.intersection(Arrays.asList(bitmaps));  // invalid result..
    System.out.println("bitset(org) took.. " + (System.currentTimeMillis() - t) + " msec");
//    Assert.assertEquals(size, u.size());

    f = new RoaringBitmapFactory();
    for (int i = 0; i < bitmaps.length; i++) {
      MutableBitmap mutable = f.makeEmptyMutableBitmap();
      IntIterator iterators = bitmaps[i].iterator();
      while (iterators.hasNext()) {
        mutable.add(iterators.next());
      }
      bitmaps[i] = f.makeImmutableBitmap(mutable);
    }
    t = System.currentTimeMillis();
    u = f.intersection(Arrays.asList(bitmaps));
    System.out.println("roaring(mod) took.. " + (System.currentTimeMillis() - t) + " msec");
    Assert.assertEquals(size, u.size());

    f = new com.metamx.collections.bitmap.RoaringBitmapFactory();
    t = System.currentTimeMillis();
    u = f.intersection(Arrays.asList(bitmaps));
    System.out.println("roaring(org) took.. " + (System.currentTimeMillis() - t) + " msec");
    Assert.assertEquals(size, u.size());
  }

  private final RoaringBitmapFactory F = new RoaringBitmapFactory();

  @Test
  public void testOptimized()
  {
    IntIterable set1 = new IntIterable.Range(1, 6);
    IntIterable set2 = new IntIterable.FromArray(new int[] {2, 4, 6, 8});
    IntIterable set3 = new IntIterable.Range(3, 7);
    IntIterable set4 = new IntIterable.FromArray(new int[] {3, 4, 9});

    ImmutableBitmap map1 = RoaringBitmapFactory.from(6, set1);
    ImmutableBitmap map2 = RoaringBitmapFactory.from(4, set2);
    ImmutableBitmap map3 = RoaringBitmapFactory.from(5, set3);
    ImmutableBitmap map4 = RoaringBitmapFactory.from(3, set4);

    check(F.union(Arrays.asList(map1, map2)), new int[]{1, 2, 3, 4, 5, 6, 8});
    check(F.union(Arrays.asList(map1, map3)), new int[]{1, 2, 3, 4, 5, 6, 7});
    check(F.union(Arrays.asList(map1, map4)), new int[]{1, 2, 3, 4, 5, 6, 9});
    check(F.union(Arrays.asList(map2, map3)), new int[]{2, 3, 4, 5, 6, 7, 8});
    check(F.union(Arrays.asList(map2, map4)), new int[]{2, 3, 4, 6, 8, 9});
    check(F.union(Arrays.asList(map3, map4)), new int[]{3, 4, 5, 6, 7, 9});

    check(F.intersection(Arrays.asList(map1, map2)), new int[]{2, 4, 6});
    check(F.intersection(Arrays.asList(map1, map3)), new int[]{3, 4, 5, 6});
    check(F.intersection(Arrays.asList(map1, map4)), new int[]{3, 4});
    check(F.intersection(Arrays.asList(map2, map3)), new int[]{4, 6});
    check(F.intersection(Arrays.asList(map2, map4)), new int[]{4});
    check(F.intersection(Arrays.asList(map3, map4)), new int[]{3, 4});
  }

  private void check(ImmutableBitmap result, int[] expected)
  {
    Assert.assertTrue(IntIterators.elementsEqual(result.iterator(), IntIterators.from(expected)));
  }
}
