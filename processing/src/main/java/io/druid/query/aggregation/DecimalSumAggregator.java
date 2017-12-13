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
import io.druid.segment.ObjectColumnSelector;

import java.math.BigDecimal;

/**
 */
public abstract class DecimalSumAggregator implements Aggregator
{
  BigDecimal sum = BigDecimal.ZERO;

  public static DecimalSumAggregator create(
      final ObjectColumnSelector<BigDecimal> selector,
      final ValueMatcher predicate
  )
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new DecimalSumAggregator()
      {
        @Override
        public final void aggregate()
        {
          BigDecimal v = selector.get();
          if (v == null) {
            return;
          }
          synchronized (this) {
            sum = sum.add(v);
          }
        }
      };
    } else {
      return new DecimalSumAggregator()
      {
        @Override
        public final void aggregate()
        {
          if (predicate.matches()) {
            BigDecimal v = selector.get();
            if (v == null) {
              return;
            }
            synchronized (this) {
              sum = sum.add(v);
            }
          }
        }
      };
    }
  }

  @Override
  public void reset()
  {
    sum = BigDecimal.ZERO;
  }

  @Override
  public Object get()
  {
    return sum;
  }

  @Override
  public float getFloat()
  {
    return sum.floatValue();
  }

  @Override
  public long getLong()
  {
    return sum.longValue();
  }

  @Override
  public double getDouble()
  {
    return sum.doubleValue();
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
