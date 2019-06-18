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

import io.druid.query.filter.ValueMatcher;
import io.druid.segment.LongColumnSelector;

import java.util.Comparator;

/**
 */
public abstract class LongMinAggregator implements Aggregator
{
  static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

  static long combineValues(Object lhs, Object rhs)
  {
    return Math.min(((Number) lhs).longValue(), ((Number) rhs).longValue());
  }

  long min = Long.MAX_VALUE;

  public static LongMinAggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new LongMinAggregator()
      {
        @Override
        public final void aggregate()
        {
          final Long v = selector.get();
          if (v != null) {
            synchronized (this) {
              min = Math.min(min, v);
            }
          }
        }
      };
    } else {
      return new LongMinAggregator()
      {
        @Override
        public final void aggregate()
        {
          if (predicate.matches()) {
            final Long v = selector.get();
            if (v != null) {
              synchronized (this) {
                min = Math.min(min, v);
              }
            }
          }
        }
      };
    }
  }

  @Override
  public void reset()
  {
    min = Long.MAX_VALUE;
  }

  @Override
  public Object get()
  {
    return min;
  }

  @Override
  public Float getFloat()
  {
    return (float) min;
  }

  @Override
  public Long getLong()
  {
    return min;
  }

  @Override
  public Double getDouble()
  {
    return (double) min;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
