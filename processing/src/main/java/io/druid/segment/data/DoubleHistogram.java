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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.segment.data.MetricBitmaps.DoubleBitmaps;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DoubleHistogram implements MetricHistogram.DoubleType
{
  private static final int DEFAULT_NUM_GROUP = 10;
  private static final int DEFAULT_COMPACT_INTERVAL = -1;

  private final BitmapFactory factory;
  private final int numSample;
  private final int numGroup;
  private final int compactInterval;

  private double[] breaks;
  private MutableBitmap[] bins;
  private final MutableBitmap zeros;

  private int count;
  private double min;
  private double max;

  private final List<DoubleWithTag> sampling = Lists.newArrayList();
  private final MutableBitmap belowMin;
  private final MutableBitmap overMax;

  public DoubleHistogram(BitmapFactory factory, int numSample)
  {
    this(factory, numSample, DEFAULT_NUM_GROUP, DEFAULT_COMPACT_INTERVAL);
  }

  public DoubleHistogram(BitmapFactory factory, int numSample, int numGroup, int compactInterval)
  {
    this.factory = factory;
    this.numSample = numSample;
    this.numGroup = numGroup;
    this.compactInterval = compactInterval;
    this.min = Double.MAX_VALUE;
    this.max = Double.MIN_VALUE;
    this.belowMin = factory.makeEmptyMutableBitmap();
    this.overMax = factory.makeEmptyMutableBitmap();
    this.zeros = factory.makeEmptyMutableBitmap();
  }

  public void offer(double d)
  {
    if (d < min) {
      min = d;
    }
    if (d > max) {
      max = d;
    }
    if (d == 0d) {
      zeros.add(count);
    }
    if (count < numSample) {
      sampling.add(new DoubleWithTag(count++, d));
      return;
    }
    if (breaks == null) {
      makeInitial();
    }

    final int position = Arrays.binarySearch(breaks, d);
    if (position == -1) {
      belowMin.add(count++);
      if (belowMin.size() > (count - belowMin.size()) / numGroup) {
        handleBelowMin();
      }
    } else if (position == bins.length || position == -breaks.length - 1) {
      overMax.add(count++);
      if (min != max && overMax.size() > (count - overMax.size()) / (numGroup * 1.5)) {
        handleOverMax();
      }
    } else {
      bins[position >= 0 ? position : -position - 2].add(count++);
    }

    if (compactInterval > 0 && count % compactInterval == 0) {
      compact(numGroup);
    }
  }

  private void makeInitial()
  {
    Collections.sort(sampling);

    breaks = new double[numGroup + 1];
    bins = new MutableBitmap[numGroup];
    for (int i = 0; i < bins.length; i++) {
      bins[i] = factory.makeEmptyMutableBitmap();
    }

    breaks[0] = min;

    int x = 0;
    int b = 0;
    int i = 1;
    final float increment = sampling.size() / (float) numGroup;
    for (; i < breaks.length; i++, b++) {
      b = Math.max(b, (int) (increment * i));
      for (; b < sampling.size() && sampling.get(b).value <= breaks[i - 1]; b++) {
      }
      if (b >= sampling.size()) {
        break;
      }
      breaks[i] = sampling.get(b).value;
      for (; x < sampling.size() && sampling.get(x).value < breaks[i]; x++) {
        bins[i - 1].add(sampling.get(x).id);
      }
    }
    for (; x < sampling.size(); x++) {
      overMax.add(sampling.get(x).id);
    }
    if (i < breaks.length) {
      breaks = Arrays.copyOfRange(breaks, 0, i);
      bins = Arrays.copyOfRange(bins, 0, i - 1);
    }
    sampling.clear();
  }

  private void handleBelowMin()
  {
    double[] newBreaks = new double[breaks.length + 1];
    MutableBitmap[] newBins = new MutableBitmap[bins.length + 1];
    System.arraycopy(breaks, 0, newBreaks, 1, breaks.length);
    System.arraycopy(bins, 0, newBins, 1, bins.length);
    newBreaks[0] = min;
    newBins[0] = factory.makeEmptyMutableBitmap();
    newBins[0].or(belowMin);
    this.breaks = newBreaks;
    this.bins = newBins;
    belowMin.clear();
  }

  private void handleOverMax()
  {
    Preconditions.checkArgument(breaks.length > 1 || max > breaks[bins.length]);
    double[] newBreaks = Arrays.copyOf(breaks, breaks.length + 1);
    MutableBitmap[] newBins = Arrays.copyOf(bins, bins.length + 1);
    newBreaks[breaks.length] = newNextMax();
    newBins[bins.length] = factory.makeEmptyMutableBitmap();
    newBins[bins.length].or(overMax);
    this.breaks = newBreaks;
    this.bins = newBins;
    overMax.clear();
  }

  // returns new break which includes max for simplicity
  private double newNextMax()
  {
    if (max == breaks[bins.length]) {
      return breaks[bins.length] + (breaks[bins.length] - breaks[bins.length - 1]);
    }
    return breaks[bins.length] + (max - breaks[bins.length]) * 2;
  }

  public DoubleBitmaps snapshot()
  {
    return snapshot(numGroup);
  }

  public DoubleBitmaps snapshot(int numGroup)
  {
    if (min == max || count == 0) {
      return null;
    }
    if (!sampling.isEmpty()) {
      makeInitial();
    }
    if (!belowMin.isEmpty()) {
      handleBelowMin();
    }
    if (!overMax.isEmpty()) {
      handleOverMax();
    }
    return toHistogram(numGroup);
  }

  private DoubleBitmaps toHistogram(int numGroup)
  {
    Preconditions.checkArgument(breaks[0] == min);
    Preconditions.checkArgument(breaks[bins.length] >= max);

    List<Double> mergedBreaks = Lists.newArrayList();
    List<MutableBitmap> mergedBins = Lists.newArrayList();

    mergedBreaks.add(min);

    int prev = 0;
    int currentSum = 0;
    final int expected = count / (numGroup + 1);
    for (int i = 0; i < bins.length; i++) {
      if (currentSum >= expected || (prev < i && bins[i].size() > expected)) {
        MutableBitmap mergee = factory.makeEmptyMutableBitmap();
        for (; prev < i; prev++) {
          mergee.or(bins[prev]);
        }
        mergedBins.add(mergee);
        mergedBreaks.add(breaks[prev]);
        currentSum = 0;
      }
      currentSum += bins[i].size();
    }

    // last one as inclusive
    MutableBitmap mergee;
    if (mergedBreaks.get(mergedBreaks.size() - 1) == max) {
      mergee = mergedBins.get(mergedBins.size() - 1);
    } else {
      mergedBins.add(mergee = factory.makeEmptyMutableBitmap());
      mergedBreaks.add(max);
    }
    for (; prev < bins.length; prev++) {
      mergee.or(bins[prev]);
    }
    List<ImmutableBitmap> immutable = Lists.transform(
        mergedBins, new Function<MutableBitmap, ImmutableBitmap>()
        {
          @Override
          public ImmutableBitmap apply(MutableBitmap input)
          {
            return factory.makeImmutableBitmap(input);
          }
        }
    );

    return new DoubleBitmaps(
        factory,
        Doubles.toArray(mergedBreaks),
        immutable.toArray(new ImmutableBitmap[immutable.size()]),
        factory.makeImmutableBitmap(zeros)
    );
  }

  private void compact(int numGroup)
  {
    List<Double> mergedBreaks = Lists.newArrayList();
    List<MutableBitmap> mergedBins = Lists.newArrayList();

    mergedBreaks.add(breaks[0]);

    int prev = 0;
    int currentSum = 0;
    final int expected = count / (numGroup + 1);
    for (int i = 0; i < bins.length; i++) {
      if (currentSum >= expected || (prev < i && bins[i].size() > expected)) {
        MutableBitmap mergee = factory.makeEmptyMutableBitmap();
        for (; prev < i; prev++) {
          mergee.or(bins[prev]);
        }
        mergedBins.add(mergee);
        mergedBreaks.add(breaks[prev]);
        currentSum = 0;
      }
      currentSum += bins[i].size();
    }
    MutableBitmap mergee = factory.makeEmptyMutableBitmap();
    for (; prev < bins.length; prev++) {
      mergee.or(bins[prev]);
    }
    mergedBins.add(mergee);
    mergedBreaks.add(breaks[prev]);

    this.breaks = Doubles.toArray(mergedBreaks);
    this.bins = mergedBins.toArray(new MutableBitmap[mergedBins.size()]);
  }

  public double[] getBreaks()
  {
    return breaks;
  }

  public MutableBitmap[] getBins()
  {
    return bins;
  }

  public double getMin()
  {
    return min;
  }

  public double getMax()
  {
    return max;
  }

  public int[] getSizes()
  {
    int[] sizes = new int[bins.length];
    for (int i = 0; i < sizes.length; i++) {
      sizes[i] = bins[i].size();
    }
    return sizes;
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder();
    builder.append("breaks :").append(Arrays.toString(getBreaks())).append('\n');
    builder.append("sizes :").append(Arrays.toString(getSizes()));
    return builder.toString();
  }

  private class DoubleWithTag implements Comparable<DoubleWithTag>
  {
    final int id;
    final double value;

    private DoubleWithTag(int id, double value)
    {
      this.id = id;
      this.value = value;
    }

    @Override
    public int compareTo(DoubleWithTag o)
    {
      int compare = Double.compare(value, o.value);
      if (compare == 0) {
        compare = Ints.compare(id, o.id);
      }
      return compare;
    }

    @Override
    public String toString()
    {
      return "[" + id + "]" + value;
    }
  }
}
