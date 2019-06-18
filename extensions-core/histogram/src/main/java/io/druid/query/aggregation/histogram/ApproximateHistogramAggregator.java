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

import com.google.common.primitives.Longs;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

public class ApproximateHistogramAggregator extends Aggregator.Abstract implements Aggregators.EstimableAggregator
{
  public static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((ApproximateHistogramHolder) o).count(), ((ApproximateHistogramHolder) o1).count());
    }
  };

  static Object combineHistograms(Object lhs, Object rhs)
  {
    return ((ApproximateHistogramHolder) lhs).foldFast((ApproximateHistogramHolder) rhs);
  }

  private final String name;
  private final FloatColumnSelector selector;
  private final int resolution;
  private final float lowerLimit;
  private final float upperLimit;
  private final boolean compact;
  private final ValueMatcher predicate;

  private ApproximateHistogramHolder histogram;

  public ApproximateHistogramAggregator(
      String name,
      FloatColumnSelector selector,
      int resolution,
      float lowerLimit,
      float upperLimit,
      boolean compact,
      ValueMatcher predicate
  )
  {
    this.name = name;
    this.selector = selector;
    this.resolution = resolution;
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.compact = compact;
    this.predicate = predicate;
    reset();
  }

  @Override
  public void aggregate()
  {
    if (predicate.matches()) {
      final Float value = selector.get();
      if (value != null) {
        histogram.offer(value);
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

  public String getName()
  {
    return name;
  }

  @Override
  public int estimateOccupation()
  {
    return histogram.estimateOccupation();
  }
}
