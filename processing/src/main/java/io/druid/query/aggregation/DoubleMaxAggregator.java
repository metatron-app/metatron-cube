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

package io.druid.query.aggregation;

import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

/**
 */
public abstract class DoubleMaxAggregator implements Aggregator
{
  static final Comparator COMPARATOR = DoubleSumAggregator.COMPARATOR;

  static double combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
  }

  double max = Double.NEGATIVE_INFINITY;

  @Override
  public void reset()
  {
    max = Double.NEGATIVE_INFINITY;
  }

  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public Float getFloat()
  {
    return (float) max;
  }

  @Override
  public Long getLong()
  {
    return (long) max;
  }

  @Override
  public Double getDouble()
  {
    return max;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  public static DoubleMaxAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new DoubleMaxAggregator()
      {
        @Override
        public final void aggregate()
        {
          final Float v = selector.get();
          if (v != null) {
            synchronized (this) {
              max = Math.max(max, v);
            }
          }
        }
      };
    } else {
      return new DoubleMaxAggregator()
      {
        @Override
        public final void aggregate()
        {
          if (predicate.matches()) {
            final Float v = selector.get();
            if (v != null) {
              synchronized (this) {
                max = Math.max(max, v);
              }
            }
          }
        }
      };
    }
  }

  public static DoubleMaxAggregator create(final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new DoubleMaxAggregator()
      {
        @Override
        public final void aggregate()
        {
          final Double v = selector.get();
          if (v != null) {
            synchronized (this) {
              max = Math.max(max, v);
            }
          }
        }
      };
    } else {
      return new DoubleMaxAggregator()
      {
        @Override
        public final void aggregate()
        {
          if (predicate.matches()) {
            final Double v = selector.get();
            if (v != null) {
              synchronized (this) {
                max = Math.max(max, v);
              }
            }
          }
        }
      };
    }
  }
}
