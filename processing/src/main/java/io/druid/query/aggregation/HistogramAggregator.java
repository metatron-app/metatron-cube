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

package io.druid.query.aggregation;

import com.google.common.primitives.Longs;
import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

public class HistogramAggregator extends Aggregator.Abstract<Histogram>
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((Histogram) o).count, ((Histogram) o1).count);
    }
  };

  static Object combineHistograms(Object lhs, Object rhs) {
    return ((Histogram) lhs).fold((Histogram) rhs);
  }

  private final FloatColumnSelector selector;

  private final float[] breaks;

  public HistogramAggregator(FloatColumnSelector selector, float[] breaks)
  {
    this.selector = selector;
    this.breaks = breaks;
  }

  @Override
  public Histogram aggregate(Histogram histogram)
  {
    final Float v = selector.get();
    if (v != null) {
      if (histogram == null) {
        histogram = new Histogram(breaks);
      }
      histogram.offer(v);
    }
    return histogram;
  }
}
