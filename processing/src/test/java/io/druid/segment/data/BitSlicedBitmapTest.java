/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.RoaringBitmapFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

public class BitSlicedBitmapTest
{
  private final BitmapFactory factory = new RoaringBitmapFactory();

  private <T extends Comparable> String find(BitSlicedBitmap<T> bitmap, Range<T> range)
  {
    return BitSlicedBitmaps.toString(bitmap.filterFor(range));
  }

  @Test
  public void testLong()
  {
    BitSlicer<Long> slicer = new BitSlicer.LongType(factory);
    for (long v : new long[]{Long.MAX_VALUE, 20000l, 15000l, 18000l, 15000l, 25000l, 30000l, Long.MIN_VALUE + 10}) {
      slicer.add(v);
    }
    BitSlicedBitmap<Long> bitmap = slicer.build();

    long v = 20000;
    System.out.println(">> " + bitmap.filterFor(Range.greaterThan(v)));
    System.out.println(">> " + bitmap.filterFor(Range.atLeast(v)));
    System.out.println(">> " + bitmap.filterFor(Range.lessThan(v)));
    System.out.println(">> " + bitmap.filterFor(Range.atMost(v)));
    System.out.println(">> " + bitmap.filterFor(Range.closed(v, v)));
  }

  @Test
  public void testFloat()
  {
    BitSlicer<Float> slicer = new BitSlicer.FloatType(factory);
    float[] values = {Float.MAX_VALUE, -1000.1f, -2000.2f, 4000.3f, 0.4f, 12000.5f, -7000.6f, 1800.7f, Float.MIN_VALUE};
    for (float v : values) {
      slicer.add(v);
    }
    BitSlicedBitmap<Float> bitmap = slicer.build();

    Assert.assertEquals("0,3,5", find(bitmap, Range.greaterThan(0.4f)));
    Assert.assertEquals("0,3,4,5", find(bitmap, Range.atLeast(0.4f)));
    Assert.assertEquals("1,2,6,7,8", find(bitmap, Range.lessThan(0.4f)));
    Assert.assertEquals("1,2,4,6,7,8", find(bitmap, Range.atMost(0.4f)));
    Assert.assertEquals("4", find(bitmap, Range.closed(0.4f, 0.4f)));

    Assert.assertEquals("0,1,3,4,5,7,8", find(bitmap, Range.atLeast(-1000.1f)));
    Assert.assertEquals("1,2,4,6,7,8", find(bitmap, Range.lessThan(4000.3f)));
    Assert.assertEquals("1,4,7,8", find(bitmap, Range.closedOpen(-1000.1f, 4000.3f)));
  }

  @Test
  public void ensureLongSorted()
  {
    BitSlicer<Long> slicer = new BitSlicer.LongType(factory);
    Random r = new Random();
    for (int x = 0; x < 32; x++) {
      Map<Long, String> values = Maps.newTreeMap();
      for (int i = 0; i < 65536; i++) {
        long v = r.nextLong();
        values.put(v, slicer.toBitmapString(v));
      }
      values.put(Long.MAX_VALUE, slicer.toBitmapString(Long.MAX_VALUE));
      values.put(Long.MIN_VALUE, slicer.toBitmapString(Long.MIN_VALUE));
      values.put(0L, slicer.toBitmapString(0L));

      String[] duplicate = values.values().toArray(new String[values.size()]);
      Arrays.parallelSort(duplicate);
      int i = 0;
      for (Map.Entry<Long, String> e : values.entrySet()) {
        Assert.assertEquals(i + " th.. " + e + " vs " + duplicate[i], e.getValue(), duplicate[i]);
        i++;
      }
    }
  }

  @Test
  public void ensureFloatSorted()
  {
    BitSlicer<Float> slicer = new BitSlicer.FloatType(factory);
    String ZERO = slicer.toBitmapString(0f);
    String POSITIVE_MIN = slicer.toBitmapString(Float.intBitsToFloat(0b0_00000000_0000000_00000000_00000001)); // MIN_VALUE
    String POSITIVE_1 = slicer.toBitmapString(Float.intBitsToFloat(0b0_00000000_1111111_11111111_11111111));
    String POSITIVE_2 = slicer.toBitmapString(Float.intBitsToFloat(0b0_00000001_0000000_00000000_00000001));  // MIN_NORMAL
    String NEGATIVE_MIN = slicer.toBitmapString(Float.intBitsToFloat(0b1_00000000_0000000_00000000_00000001));
    String NEGATIVE_1 = slicer.toBitmapString(Float.intBitsToFloat(0b1_00000000_1111111_11111111_11111111));
    String NEGATIVE_2 = slicer.toBitmapString(Float.intBitsToFloat(0b1_00000001_0000000_00000000_00000001));

    String MAX_VALUE = slicer.toBitmapString(Float.MAX_VALUE);
    String POSITIVE_INFINITY = slicer.toBitmapString(Float.POSITIVE_INFINITY);
    String MIN_VALUE = slicer.toBitmapString(-Float.MAX_VALUE);
    String NEGATIVE_INFINITY = slicer.toBitmapString(Float.NEGATIVE_INFINITY);

    String NAN = slicer.toBitmapString(Float.NaN);
    Assert.assertEquals(NAN, slicer.toBitmapString(-Float.NaN));

    String[] expected = new String[]{
        NAN,
        NEGATIVE_INFINITY,
        MIN_VALUE,
        NEGATIVE_2,
        NEGATIVE_1,
        NEGATIVE_MIN,
        ZERO,
        POSITIVE_MIN,
        POSITIVE_1,
        POSITIVE_2,
        MAX_VALUE,
        POSITIVE_INFINITY,
    };
    String[] array = Arrays.copyOf(expected, expected.length);
    for (int x = 0; x < 10; x++) {
      Collections.shuffle(Arrays.asList(array));
      Arrays.sort(array);
    }
    Assert.assertArrayEquals(expected, array);
    for (int i = 1; i < array.length; i++) {
      Assert.assertTrue(array[i - 1].compareTo(array[i]) != 0);
    }
  }

  @Test
  @Ignore("takes too much time")
  public void ensureSorted()
  {
    BitSlicer<Float> slicer = new BitSlicer.FloatType(factory);
    String prev = null;
    for (int i = 0; i <= Float.floatToIntBits(Float.POSITIVE_INFINITY); i++) {
      String c = slicer.toBitmapString(Float.intBitsToFloat(i));
      if (prev != null && c.compareTo(prev) <= 0) {
        throw new RuntimeException("[BitSlicedBitmapTest/ensureSorted] " + i + " --> " + c + " (" + prev + ")");
      }
      prev = c;
      if (i % 1_0000_0000 == 0) {
        System.out.print("x");
      } else if (i % 100_0000 == 0) {
        System.out.print(".");
      }
    }
    prev = null;
    for (int i = 0; i <= Float.floatToIntBits(Float.POSITIVE_INFINITY); i++) {
      String c = slicer.toBitmapString(-Float.intBitsToFloat(i));
      if (prev != null && c.compareTo(prev) >= 0) {
        throw new RuntimeException("[BitSlicedBitmapTest/ensureSorted] " + i + " --> " + c + " (" + prev + ")");
      }
      prev = c;
      if (i % 1_0000_0000 == 0) {
        System.out.print("x");
      } else if (i % 100_0000 == 0) {
        System.out.print(".");
      }
    }
  }

  @Test
  public void ensureDoubleSorted()
  {
    BitSlicer<Double> slicer = new BitSlicer.DoubleType(factory);
    Random r = new Random();
    final int expDelta = Double.MAX_EXPONENT - Double.MIN_EXPONENT;
    for (int x = 0; x < 32; x++) {
      Map<Double, String> values = Maps.newTreeMap();
      for (int i = 0; i < 65536; i++) {
        double v = (r.nextDouble() - 0.5f) * Math.pow(10, r.nextInt(expDelta) - Double.MAX_EXPONENT);
        values.put(v, slicer.toBitmapString(v));
      }
      values.put(Double.MAX_VALUE, slicer.toBitmapString(Double.MAX_VALUE));
      values.put(Double.MIN_VALUE, slicer.toBitmapString(Double.MIN_VALUE));
      values.put(Double.MIN_NORMAL, slicer.toBitmapString(Double.MIN_NORMAL));
      values.put(Double.longBitsToDouble(0x01), slicer.toBitmapString(Double.longBitsToDouble(0x01)));
      values.put(
          Double.longBitsToDouble(0x8000000000000001L), slicer.toBitmapString(
              Double.longBitsToDouble(
                  0x8000000000000001L
              )
          )
      );
      values.put(0D, slicer.toBitmapString(0D));
      values.put(-0D, slicer.toBitmapString(-0D));

      String[] duplicate = values.values().toArray(new String[values.size()]);
      Arrays.parallelSort(duplicate);
      int i = 0;
      for (Map.Entry<Double, String> e : values.entrySet()) {
        Assert.assertEquals(i + " th.. " + e + " vs " + duplicate[i], e.getValue(), duplicate[i]);
        i++;
      }
    }
  }
}