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

import com.google.common.collect.Ordering;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

/**
 */
public abstract class FloatMaxAggregator implements Aggregator
{
  static final Comparator COMPARATOR = new Ordering()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Float.compare(((Number) o).floatValue(), ((Number) o1).floatValue());
    }
  }.nullsFirst();

  static float combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number) lhs).floatValue(), ((Number) rhs).floatValue());
  }

  float max = Float.NEGATIVE_INFINITY;

  @Override
  public void reset()
  {
    max = Float.NEGATIVE_INFINITY;
  }

  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public float getFloat()
  {
    return max;
  }

  @Override
  public long getLong()
  {
    return (long) max;
  }

  @Override
  public double getDouble()
  {
    return max;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  public static FloatMaxAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new FloatMaxAggregator()
      {
        @Override
        public final void aggregate()
        {
          float v = selector.get();
          synchronized (this) {
            max = Math.max(max, v);
          }
        }
      };
    } else {
      return new FloatMaxAggregator()
      {
        @Override
        public final void aggregate()
        {
          if (predicate.matches()) {
            float v = selector.get();
            synchronized (this) {
              max = Math.max(max, v);
            }
          }
        }
      };
    }
  }
}
