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

import com.google.common.primitives.Longs;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.LongColumnSelector;

import java.util.Comparator;

/**
 */
public abstract class LongSumAggregator implements Aggregator
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((Number) o).longValue(), ((Number) o1).longValue());
    }
  };

  static long combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).longValue() + ((Number) rhs).longValue();
  }

  long sum = 0;

  public static LongSumAggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new LongSumAggregator()
      {
        @Override
        public final void aggregate()
        {
          sum += selector.get();
        }
      };
    } else {
      return new LongSumAggregator()
      {
        @Override
        public final void aggregate()
        {
          if (predicate.matches()) {
            sum += selector.get();
          }
        }
      };
    }
  }

  @Override
  public void reset()
  {
    sum = 0;
  }

  @Override
  public Object get()
  {
    return sum;
  }

  @Override
  public float getFloat()
  {
    return sum;
  }

  @Override
  public long getLong()
  {
    return sum;
  }

  @Override
  public double getDouble()
  {
    return sum;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
