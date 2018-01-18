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
import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

/**
 */
public abstract class FloatMinAggregator implements Aggregator
{
  static final Comparator COMPARATOR = FloatMaxAggregator.COMPARATOR;

  static float combineValues(Object lhs, Object rhs)
  {
    return Math.min(((Number) lhs).floatValue(), ((Number) rhs).floatValue());
  }

  float min = Float.POSITIVE_INFINITY;

  @Override
  public void reset()
  {
    min = Float.POSITIVE_INFINITY;
  }

  @Override
  public Object get()
  {
    return min;
  }

  @Override
  public float getFloat()
  {
    return min;
  }

  @Override
  public long getLong()
  {
    return (long) min;
  }

  @Override
  public double getDouble()
  {
    return min;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  public static FloatMinAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new FloatMinAggregator()
      {
        @Override
        public final void aggregate()
        {
          float v = selector.get();
          synchronized (this) {
            min = Math.min(min, v);
          }
        }
      };
    } else {
      return new FloatMinAggregator()
      {
        @Override
        public final void aggregate()
        {
          if (predicate.matches()) {
            float v = selector.get();
            synchronized (this) {
              min = Math.min(min, v);
            }
          }
        }
      };
    }
  }
}
