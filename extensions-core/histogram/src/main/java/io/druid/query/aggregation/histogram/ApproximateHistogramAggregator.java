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

import io.druid.query.aggregation.Aggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

public class ApproximateHistogramAggregator implements Aggregator.Simple<ApproximateHistogramHolder>,
    Aggregator.Estimable<ApproximateHistogramHolder>
{
  public static final Comparator<ApproximateHistogramHolder> COMPARATOR = new Comparator<ApproximateHistogramHolder>()
  {
    @Override
    public int compare(ApproximateHistogramHolder o, ApproximateHistogramHolder o1)
    {
      return Long.compare(o.count(), o1.count());
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
  }

  @Override
  public ApproximateHistogramHolder aggregate(ApproximateHistogramHolder current)
  {
    if (predicate.matches()) {
      final Float value = selector.get();
      if (value == null) {
        return current;
      }
      if (current == null) {
        current = newInstance();
      }
      current.offer(value);
    }
    return current;
  }

  public String getName()
  {
    return name;
  }

  @Override
  public int estimateOccupation(ApproximateHistogramHolder current)
  {
    return current == null ? 0 : current.estimateOccupation();
  }

  private ApproximateHistogramHolder newInstance()
  {
    return compact ? new ApproximateCompactHistogram(resolution, lowerLimit, upperLimit)
                   : new ApproximateHistogram(resolution, lowerLimit, upperLimit);
  }
}
