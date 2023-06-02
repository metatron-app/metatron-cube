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

import com.google.common.io.Files;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.segment.bitmap.BitSetBitmapFactory;
import io.druid.segment.bitmap.IntIterable;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.bitmap.WrappedImmutableRoaringBitmap;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;

public class BitmapTest
{
  @Test
  public void testOr() throws IOException
  {
    int range = 200_0000;
    Random r = new Random();

    int c1 = 0, c2 = 0, c3 = 0;
    MutableRoaringBitmap[] bitmaps = new MutableRoaringBitmap[10_0000];
    for (int i = 0; i < bitmaps.length; i++) {
      int c = r.nextInt(12) + 3;
      boolean x = r.nextInt(10) >= 9;
      if (c >= 12) {
        c1++;
      } else if (x) {
        c2++;
      } else {
        c3++;
      }
      bitmaps[i] = new MutableRoaringBitmap();
      if (x) {
        int s = r.nextInt(range - c);
        bitmaps[i].add(s, s + c);
      } else {
        for (int j = 0; j < c; j++) {
          bitmaps[i].add(r.nextInt(range));
        }
      }
    }
    System.out.printf("roaring %,d, range %,d, set %,d\n", c1, c2, c3);
    BitmapFactory f;
    ImmutableBitmap[] testor;

    f = new com.metamx.collections.bitmap.RoaringBitmapFactory();
    testor = toMapped(bitmaps, m -> new com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap(m), f);

    long t2 = System.currentTimeMillis();
    ImmutableBitmap u2 = f.union(Arrays.asList(testor));
    System.out.println("roaring(org) took.. " + (System.currentTimeMillis() - t2) + " msec");

    f = new RoaringBitmapFactory();
    testor = toMapped(bitmaps, m -> new WrappedImmutableRoaringBitmap(m), f);

    long t1 = System.currentTimeMillis();
    ImmutableBitmap u1 = f.union(Arrays.asList(testor));
    System.out.println("roaring(mod) took.. " + (System.currentTimeMillis() - t1) + " msec");

    Assert.assertEquals(u1.size(), u2.size());
  }

  private ImmutableBitmap[] toMapped(
      MutableRoaringBitmap[] bitmaps,
      Function<MutableRoaringBitmap, ImmutableBitmap> func,
      BitmapFactory f
  ) throws IOException
  {
    File file = File.createTempFile("__test", ".segment");
    FileOutputStream out = new FileOutputStream(file);
    ImmutableBitmap[] immutable = Arrays.stream(bitmaps).map(func).toArray(x -> new ImmutableBitmap[x]);
    int[] offsets = new int[immutable.length + 1];
    for (int i = 0; i < bitmaps.length; i++) {
      byte[] bytes = immutable[i].toBytes();
      out.write(bytes);
      offsets[i + 1] = offsets[i] + bytes.length;
    }
    System.out.printf("length = %,d bytes%n", offsets[bitmaps.length]);
    out.close();

    ByteBuffer mapped = Files.map(file).asReadOnlyBuffer();
    ImmutableBitmap[] testor = new ImmutableBitmap[bitmaps.length];
    for (int i = 0; i < bitmaps.length; i++) {
      ByteBuffer slice = ((ByteBuffer) mapped.limit(offsets[i + 1]).position(offsets[i])).slice();
      testor[i] = f.mapImmutableBitmap(slice);
    }
    return testor;
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
    u = f.intersection(Arrays.asList(bitmaps));
    System.out.println("bitset(org) took.. " + (System.currentTimeMillis() - t) + " msec");
//    Assert.assertEquals(size, u.size());    // invalid result..

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
    System.out.println("roaring(org) took.. " + (System.currentTimeMillis() - t) + " msec");
    Assert.assertEquals(size, u.size());
  }

  private final RoaringBitmapFactory F = new RoaringBitmapFactory();

  @Test
  public void testOptimized()
  {
    ImmutableBitmap map1 = RoaringBitmapFactory.from(1, 7);
    ImmutableBitmap map2 = RoaringBitmapFactory.from(new int[] {2, 4, 6, 8});
    ImmutableBitmap map3 = RoaringBitmapFactory.from(3, 8);
    ImmutableBitmap map4 = RoaringBitmapFactory.from(new int[] {3, 4, 9});

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
