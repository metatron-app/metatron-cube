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

package io.druid.query.aggregation.histogram;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class ApproximateHistogramTest
{
  static final float[] VALUES = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45};
  static final float[] VALUES2 = {23, 19, 10, 16, 36, 2, 1, 9, 32, 30, 45, 46};

  static final float[] VALUES3 = {
      20, 16, 19, 27, 17, 20, 18, 20, 28, 14, 17, 21, 20, 21, 10, 25, 23, 17, 21, 18,
      14, 20, 18, 12, 19, 20, 23, 25, 15, 22, 14, 17, 15, 23, 23, 15, 27, 20, 17, 15
  };
  static final float[] VALUES4 = {
      27.489f, 3.085f, 3.722f, 66.875f, 30.998f, -8.193f, 5.395f, 5.109f, 10.944f, 54.75f,
      14.092f, 15.604f, 52.856f, 66.034f, 22.004f, -14.682f, -50.985f, 2.872f, 61.013f,
      -21.766f, 19.172f, 62.882f, 33.537f, 21.081f, 67.115f, 44.789f, 64.1f, 20.911f,
      -6.553f, 2.178f
  };
  static final float[] VALUES5 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  static final float[] VALUES6 = {1f, 1.5f, 2f, 2.5f, 3f, 3.5f, 4f, 4.5f, 5f, 5.5f, 6f, 6.5f, 7f, 7.5f, 8f, 8.5f, 9f, 9.5f, 10f};

  protected ApproximateHistogramHolder buildHistogram(int size)
  {
    return new ApproximateHistogram(size);
  }

  private ApproximateHistogramHolder buildHistogram(int size, float[] values)
  {
    ApproximateHistogramHolder h = buildHistogram(size);
    for (float v : values) {
      h.offer(v);
    }
    return h;
  }

  protected ApproximateHistogramHolder buildHistogram(int size, float[] values, float lowerLimit, float upperLimit)
  {
    ApproximateHistogramHolder h = new ApproximateHistogram(size, lowerLimit, upperLimit);
    for (float v : values) {
      h.offer(v);
    }
    return h;
  }

  protected ApproximateHistogramHolder buildHistogram(int binCount, float[] positions, long[] bins, float min, float max)
  {
    return new ApproximateHistogram(binCount, positions, bins, min, max);
  }

  protected ApproximateHistogramHolder buildHistogram(byte[] buffer)
  {
    return new ApproximateHistogram().fromBytes(buffer);
  }

  @Test
  public void testOffer() throws Exception
  {
    ApproximateHistogramHolder h = buildHistogram(5, VALUES);

    // (2, 1), (9.5, 2), (19.33, 3), (32.67, 3), (45, 1)
    Assert.assertArrayEquals(
        "final bin positions match expected positions",
        new float[]{2, 9.5f, 19.33f, 32.67f, 45f}, h.positions(), 0.1f
    );

    Assert.assertArrayEquals(
        "final bin positions match expected positions",
        new long[]{1, 2, 3, 3, 1}, h.bins()
    );

    Assert.assertEquals("min value matches expected min", 2, h.min(), 0);
    Assert.assertEquals("max value matches expected max", 45, h.max(), 0);

    Assert.assertEquals("bin count matches expected bin count", 5, h.binCount());
  }

  @Test
  public void testFold()
  {
    ApproximateHistogramHolder merged = buildHistogram(0);
    ApproximateHistogramHolder mergedFast = buildHistogram(0);
    ApproximateHistogramHolder h1 = buildHistogram(5);
    ApproximateHistogramHolder h2 = buildHistogram(10);

    for (int i = 0; i < 5; ++i) {
      h1.offer(VALUES[i]);
    }
    for (int i = 5; i < VALUES.length; ++i) {
      h2.offer(VALUES[i]);
    }

    merged.fold(h1);
    merged.fold(h2);
    mergedFast.foldFast(h1);
    mergedFast.foldFast(h2);

    Assert.assertArrayEquals(
        "final bin positions match expected positions",
        new float[]{2, 9.5f, 19.33f, 32.67f, 45f}, merged.positions(), 0.1f
    );
    Assert.assertArrayEquals(
        "final bin positions match expected positions",
        new float[]{11.2f, 30.25f, 45f}, mergedFast.positions(), 0.1f
    );

    Assert.assertArrayEquals(
        "final bin counts match expected counts",
        new long[]{1, 2, 3, 3, 1}, merged.bins()
    );
    Assert.assertArrayEquals(
        "final bin counts match expected counts",
        new long[]{5, 4, 1}, mergedFast.bins()
    );

    Assert.assertEquals("merged max matches expected value", 45f, merged.max(), 0.1f);
    Assert.assertEquals("mergedfast max matches expected value", 45f, mergedFast.max(), 0.1f);
    Assert.assertEquals("merged min matches expected value", 2f, merged.min(), 0.1f);
    Assert.assertEquals("mergedfast min matches expected value", 2f, mergedFast.min(), 0.1f);

    // fold where merged bincount is less than total bincount
    ApproximateHistogramHolder a = buildHistogram(10, new float[]{1, 2, 3, 4, 5, 6});
    ApproximateHistogramHolder aFast = buildHistogram(10, new float[]{1, 2, 3, 4, 5, 6});
    ApproximateHistogramHolder b = buildHistogram(5, new float[]{3, 4, 5, 6});

    a.fold(b);
    aFast.foldFast(b);

    Assert.assertEquals(
        buildHistogram(
            6,
            new float[]{1, 2, 3, 4, 5, 6, 0, 0, 0, 0},
            new long[]{1, 1, 2, 2, 2, 2, 0, 0, 0, 0},
            1, 6
        ), a
    );
    Assert.assertEquals(
        buildHistogram(
            6,
            new float[]{1, 2, 3, 4, 5, 6, 0, 0, 0, 0},
            new long[]{1, 1, 2, 2, 2, 2, 0, 0, 0, 0},
            1, 6
        ), aFast
    );

    ApproximateHistogramHolder h3 = buildHistogram(10);
    ApproximateHistogramHolder h4 = buildHistogram(10);
    for (float v : VALUES3) {
      h3.offer(v);
    }
    for (float v : VALUES4) {
      h4.offer(v);
    }
    h3.fold(h4);
    Assert.assertArrayEquals(
        "final bin positions match expected positions",
        new float[]{-50.98f, -21.77f, -9.81f, 3.73f, 13.72f, 20.1f, 29f, 44.79f, 53.8f, 64.67f},
        h3.positions(), 0.1f
    );
    Assert.assertArrayEquals(
        "final bin counts match expected counts",
        new long[]{1, 1, 3, 6, 12, 32, 6, 1, 2, 6}, h3.bins()
    );

  }

  @Test
  public void testFoldNothing() throws Exception
  {
    ApproximateHistogramHolder h1 = buildHistogram(10);
    ApproximateHistogramHolder h2 = buildHistogram(10);

    h1.fold(h2);
    h1.foldFast(h2);
  }

  @Test
  public void testFoldNothing2() throws Exception
  {
    ApproximateHistogramHolder h1 = buildHistogram(10);
    ApproximateHistogramHolder h1Fast = buildHistogram(10);
    ApproximateHistogramHolder h2 = buildHistogram(10);
    ApproximateHistogramHolder h3 = buildHistogram(10);
    ApproximateHistogramHolder h4 = buildHistogram(10);
    ApproximateHistogramHolder h4Fast = buildHistogram(10);
    for (float v : VALUES3) {
      h3.offer(v);
      h4.offer(v);
      h4Fast.offer(v);
    }

    h1.fold(h3);
    h4.fold(h2);
    h1Fast.foldFast(h3);
    h4Fast.foldFast(h2);

    Assert.assertEquals(h3, h1);
    Assert.assertEquals(h4, h3);
    Assert.assertEquals(h3, h1Fast);
    Assert.assertEquals(h3, h4Fast);
  }

  //@Test
  public void testFoldSpeed()
  {
    final int combinedHistSize = 200;
    final int histSize = 50;
    final int numRand = 10000;
    ApproximateHistogramHolder h = buildHistogram(combinedHistSize);
    Random rand = new Random(0);
    //for(int i = 0; i < 200; ++i) h.offer((float)(rand.nextGaussian() * 50.0));
    long tFold = 0;
    int count = 5000000;

    List<ApproximateHistogramHolder> randHist = Lists.newLinkedList();
    Iterator<ApproximateHistogramHolder> it = Iterators.cycle(randHist);

    for (int k = 0; k < numRand; ++k) {
      ApproximateHistogramHolder tmp = buildHistogram(histSize);
      for (int i = 0; i < 20; ++i) {
        tmp.offer((float) (rand.nextGaussian() + (double) k));
      }
      randHist.add(tmp);
    }

    float[] mergeBufferP = new float[combinedHistSize * 2];
    long[] mergeBufferB = new long[combinedHistSize * 2];

    for (int i = 0; i < count; ++i) {
      ApproximateHistogramHolder tmp = it.next();

      long t0 = System.nanoTime();
      //h.fold(tmp, mergeBufferP, mergeBufferB, mergeBufferD);
      h.foldFast(tmp, mergeBufferP, mergeBufferB);
      tFold += System.nanoTime() - t0;
    }

    System.out.println(String.format("Average folds per second : %f", (double) count / (double) tFold * 1e9));
  }

  @Test
  public void testSum()
  {
    ApproximateHistogramHolder h = buildHistogram(5, VALUES);

    Assert.assertEquals(0.0f, h.sum(0), 0.01);
    Assert.assertEquals(1.0f, h.sum(2), 0.01);
    Assert.assertEquals(1.16f, h.sum(5), 0.01);
    Assert.assertEquals(3.28f, h.sum(15), 0.01);
    Assert.assertEquals(VALUES.length, h.sum(45), 0.01);
    Assert.assertEquals(VALUES.length, h.sum(46), 0.01);

    ApproximateHistogramHolder h2 = buildHistogram(5, VALUES2);

    Assert.assertEquals(0.0f, h2.sum(0), 0.01);
    Assert.assertEquals(0.0f, h2.sum(1f), 0.01);
    Assert.assertEquals(1.0f, h2.sum(1.5f), 0.01);
    Assert.assertEquals(1.125f, h2.sum(2f), 0.001);
    Assert.assertEquals(2.0625f, h2.sum(5.75f), 0.001);
    Assert.assertEquals(3.0f, h2.sum(9.5f), 0.01);
    Assert.assertEquals(11.0f, h2.sum(45.5f), 0.01);
    Assert.assertEquals(12.0f, h2.sum(46f), 0.01);
    Assert.assertEquals(12.0f, h2.sum(47f), 0.01);
  }

  @Test
  public void testSerializeCompact()
  {
    ApproximateHistogramHolder h = buildHistogram(5, VALUES);
    Assert.assertEquals(h, buildHistogram(h.toBytes()));

    ApproximateHistogramHolder h2 = buildHistogram(50).fold(h);
    Assert.assertEquals(h2, buildHistogram(h2.toBytes()));
  }

  @Test
  public void testSerializeDense()
  {
    ApproximateHistogram h = (ApproximateHistogram) buildHistogram(5, VALUES);
    ByteBuffer buf = ByteBuffer.allocate(h.getDenseStorageSize());
    h.toBytesDense(buf);
    Assert.assertEquals(h, buildHistogram(buf.array()));
  }

  @Test
  public void testSerializeSparse()
  {
    ApproximateHistogram h = (ApproximateHistogram) buildHistogram(5, VALUES);
    ByteBuffer buf = ByteBuffer.allocate(h.getSparseStorageSize());
    h.toBytesSparse(buf);
    Assert.assertEquals(h, buildHistogram(buf.array()));
  }

  @Test
  public void testSerializeCompactExact()
  {
    ApproximateHistogramHolder h = buildHistogram(50, new float[]{1f, 2f, 3f, 4f, 5f});
    Assert.assertEquals(h, buildHistogram(h.toBytes()));

    h = buildHistogram(5, new float[]{1f, 2f, 3f});
    Assert.assertEquals(h, buildHistogram(h.toBytes()));

    h = buildHistogram(40).fold(h);
    Assert.assertEquals(h, buildHistogram(h.toBytes()));
  }

  @Test
  public void testSerializeEmpty()
  {
    ApproximateHistogramHolder h = buildHistogram(50);
    Assert.assertEquals(h, buildHistogram(h.toBytes()));
  }

  @Test
  public void testQuantileSmaller()
  {
    ApproximateHistogramHolder h = buildHistogram(20, VALUES5);
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{5f},
        h.getQuantiles(new float[]{.5f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{3.33f, 6.67f},
        h.getQuantiles(new float[]{.333f, .666f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{2.5f, 5f, 7.5f},
        h.getQuantiles(new float[]{.25f, .5f, .75f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{2f, 4f, 6f, 8f},
        h.getQuantiles(new float[]{.2f, .4f, .6f, .8f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f},
        h.getQuantiles(new float[]{.1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f}), 0.1f
    );
  }

  @Test
  public void testQuantileEqualSize()
  {
    ApproximateHistogramHolder h = buildHistogram(10, VALUES5);
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{5f},
        h.getQuantiles(new float[]{.5f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{3.33f, 6.67f},
        h.getQuantiles(new float[]{.333f, .666f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{2.5f, 5f, 7.5f},
        h.getQuantiles(new float[]{.25f, .5f, .75f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{2f, 4f, 6f, 8f},
        h.getQuantiles(new float[]{.2f, .4f, .6f, .8f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f},
        h.getQuantiles(new float[]{.1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f}), 0.1f
    );
  }

  @Test
  public void testQuantileBigger()
  {
    ApproximateHistogramHolder h = buildHistogram(5, VALUES5);
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{4.5f},
        h.getQuantiles(new float[]{.5f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{2.83f, 6.17f},
        h.getQuantiles(new float[]{.333f, .666f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{2f, 4.5f, 7f},
        h.getQuantiles(new float[]{.25f, .5f, .75f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{1.5f, 3.5f, 5.5f, 7.5f},
        h.getQuantiles(new float[]{.2f, .4f, .6f, .8f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{1f, 1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f, 7.5f, 8.5f},
        h.getQuantiles(new float[]{.1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f}), 0.1f
    );
  }

  @Test
  public void testQuantileBigger2()
  {
    float[] thousand = new float[1000];
    for (int i = 1; i <= 1000; ++i) {
      thousand[i - 1] = i;
    }
    ApproximateHistogramHolder h = buildHistogram(100, thousand);

    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{493.5f},
        h.getQuantiles(new float[]{.5f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{327.5f, 662f},
        h.getQuantiles(new float[]{.333f, .666f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{244.5f, 493.5f, 746f},
        h.getQuantiles(new float[]{.25f, .5f, .75f}), 0.1f
    );
    Assert.assertArrayEquals(
        "expected quantiles match actual quantiles",
        new float[]{96.5f, 196.53f, 294.5f, 395.5f, 493.5f, 597f, 696f, 795f, 895.25f},
        h.getQuantiles(new float[]{.1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f}), 0.1f
    );
  }

  @Test
  public void testLimitSum()
  {
    final float lowerLimit = 0f;
    final float upperLimit = 10f;

    ApproximateHistogramHolder h = buildHistogram(15, VALUES6, lowerLimit, upperLimit);

    for (int i = 1; i <= 20; ++i) {
      ApproximateHistogramHolder hLow = buildHistogram(5);
      ApproximateHistogramHolder hHigh = buildHistogram(5);
      hLow.offer(lowerLimit - i);
      hHigh.offer(upperLimit + i);
      h.foldFast(hLow);
      h.foldFast(hHigh);
    }

    Assert.assertEquals(20f, h.sum(lowerLimit), .7f);
    Assert.assertEquals(VALUES6.length + 20f, h.sum(upperLimit), 0.01);
  }

  @Test
  public void testBuckets()
  {
    final float[] values = new float[]{-5f, .01f, .02f, .06f, .12f, 1f, 2f};
    ApproximateHistogramHolder h = buildHistogram(50, values, 0f, 1f);
    Histogram h2 = h.toHistogram(.05f, 0f);

    Assert.assertArrayEquals(
        "expected counts match actual counts",
        new double[]{1f, 2f, 1f, 1f, 0f, 1f, 1f},
        h2.getCounts(), 0.1f
    );

    Assert.assertArrayEquals(
        "expected breaks match actual breaks",
        new double[]{-5.05f, 0f, .05f, .1f, .15f, .95f, 1f, 2f},
        h2.getBreaks(), 0.1f
    );
  }

  @Test
  public void testBuckets2()
  {
    final float[] values = new float[]{-5f, .01f, .02f, .06f, .12f, .94f, 1f, 2f};
    ApproximateHistogramHolder h = buildHistogram(50, values, 0f, 1f);
    Histogram h2 = h.toHistogram(.05f, 0f);

    Assert.assertArrayEquals(
        "expected counts match actual counts",
        new double[]{1f, 2f, 1f, 1f, 0f, 1f, 1f, 1f},
        h2.getCounts(), 0.1f
    );

    Assert.assertArrayEquals(
        "expected breaks match actual breaks",
        new double[]{-5.05f, 0f, .05f, .1f, .15f, .9f, .95f, 1f, 2.05f},
        h2.getBreaks(), 0.1f
    );
  }

  @Test
  public void testBuckets3()
  {
    final float[] values = new float[]{0f, 0f, .02f, .06f, .12f, .94f};
    ApproximateHistogramHolder h = buildHistogram(50, values, 0f, 1f);
    Histogram h2 = h.toHistogram(1f, 0f);

    Assert.assertArrayEquals(
        "expected counts match actual counts",
        new double[]{2f, 4f},
        h2.getCounts(), 0.1f
    );

    Assert.assertArrayEquals(
        "expected breaks match actual breaks",
        new double[]{-1f, 0f, 1f},
        h2.getBreaks(), 0.1f
    );
  }

  @Test
  public void testBuckets4()
  {
    final float[] values = new float[]{0f, 0f, 0.01f, 0.51f, 0.6f, 0.8f};
    ApproximateHistogramHolder h = buildHistogram(50, values, 0.5f, 1f);
    Histogram h3 = h.toHistogram(0.2f, 0);

    Assert.assertArrayEquals(
        "Expected counts match actual counts",
        new double[]{3f, 2f, 1f},
        h3.getCounts(),
        0.1f
    );

    Assert.assertArrayEquals(
        "expected breaks match actual breaks",
        new double[]{-0.2f, 0.5f, 0.7f, 0.9f},
        h3.getBreaks(), 0.1f
    );
  }

  @Test
  public void testBuckets5()
  {
    final float[] values = new float[]{0.1f, 0.5f, 0.6f};
    ApproximateHistogramHolder h = buildHistogram(50, values, 0f, 1f);
    Histogram h4 = h.toHistogram(0.5f, 0);

    Assert.assertArrayEquals(
        "Expected counts match actual counts",
        new double[]{2, 1},
        h4.getCounts(),
        0.1f
    );

    Assert.assertArrayEquals(
        "Expected breaks match actual breaks",
        new double[]{0f, 0.5f, 1f},
        h4.getBreaks(),
        0.1f
    );
  }

  @Test
  public void testEmptyHistogram()
  {
    ApproximateHistogramHolder h = buildHistogram(50);
    Assert.assertArrayEquals(
        new float[]{Float.NaN, Float.NaN},
        h.getQuantiles(new float[]{0.8f, 0.9f}),
        1e-9f
    );
  }

  @Test
  public void testFoldFastHistogramToVerifyResoultionIntact()
  {
    float[] h1Positions = new float[]{0.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f, 10.0f, 11.0f, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    long[] h1Bins = new long[]{31, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ApproximateHistogram h1 = new ApproximateHistogram(50,
                                                       h1Positions,
                                                       h1Bins,
                                                       48,
                                                       0.0f,
                                                       11.0f,
                                                       48,
                                                       Float.NEGATIVE_INFINITY,
                                                       Float.POSITIVE_INFINITY);
    long expectedH1Size = h1.size;

    float[] h2Positions = new float[]{676.309f, 677.722f, 681.059f, 683.489f, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    long[] h2Bins = new long[]{1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ApproximateHistogram h2 = new ApproximateHistogram(50,
                                                       h2Positions,
                                                       h2Bins,
                                                       4,
                                                       676.309f,
                                                       683.489f,
                                                       4,
                                                       Float.NEGATIVE_INFINITY,
                                                       Float.POSITIVE_INFINITY);

    h1.foldFast(h2);
    Assert.assertEquals(expectedH1Size, h1.size);
  }
}
