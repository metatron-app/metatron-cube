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

package io.druid.query.aggregation.histogram;


import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ObjectColumnSelector;

public class ApproximateHistogramFoldingAggregator extends Aggregator.Abstract
    implements Aggregators.EstimableAggregator
{
  private final ObjectColumnSelector<ApproximateHistogramHolder> selector;
  private final int resolution;
  private final float lowerLimit;
  private final float upperLimit;

  private ApproximateHistogramHolder histogram;
  private float[] tmpBufferP;
  private long[] tmpBufferB;

  private final boolean compact;
  private final ValueMatcher predicate;

  public ApproximateHistogramFoldingAggregator(
      ObjectColumnSelector<ApproximateHistogramHolder> selector,
      int resolution,
      float lowerLimit,
      float upperLimit,
      boolean compact,
      ValueMatcher predicate
  )
  {
    this.selector = selector;
    this.resolution = resolution;
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.predicate = predicate;
    this.compact = compact;
    reset();

    tmpBufferP = new float[resolution];
    tmpBufferB = new long[resolution];
  }

  @Override
  public void aggregate()
  {
    if (predicate.matches()) {
      ApproximateHistogramHolder h = selector.get();
      if (h == null) {
        return;
      }

      synchronized (this) {
        if (h.binCount() + histogram.binCount() <= tmpBufferB.length) {
          histogram.foldFast(h, tmpBufferP, tmpBufferB);
        } else {
          histogram.foldFast(h);
        }
      }
    }
  }

  @Override
  public void reset()
  {
    this.histogram = compact ? new ApproximateCompactHistogram(resolution, lowerLimit, upperLimit)
                             : new ApproximateHistogram(resolution, lowerLimit, upperLimit);
  }

  @Override
  public Object get()
  {
    return histogram;
  }

  @Override
  public int estimateOccupation()
  {
    return histogram.estimateOccupation();
  }
}
