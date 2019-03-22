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

package io.druid.query.aggregation.variance;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;

/**
 */
public abstract class VarianceAggregator extends Aggregator.Abstract
{
  protected final VarianceAggregatorCollector holder = new VarianceAggregatorCollector();

  @Override
  public void reset()
  {
    holder.reset();
  }

  @Override
  public Object get()
  {
    return holder;
  }

  public static Aggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return Aggregators.noopAggregator();
    }
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new VarianceAggregator()
      {
        @Override
        public void aggregate()
        {
          final Float v = selector.get();
          if (v != null) {
            holder.add(v);
          }
        }
      };
    } else {
      return new VarianceAggregator()
      {
        @Override
        public void aggregate()
        {
          if (predicate.matches()) {
            final Float v = selector.get();
            if (v != null) {
              holder.add(v);
            }
          }
        }
      };
    }
  }

  public static Aggregator create(final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return Aggregators.noopAggregator();
    }
    return new VarianceAggregator()
    {
      @Override
      public void aggregate()
      {
        if (predicate.matches()) {
          final Double v = selector.get();
          if (v != null) {
            holder.add(v);
          }
        }
      }
    };
  }

  public static Aggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return Aggregators.noopAggregator();
    }
    return new VarianceAggregator()
    {
      @Override
      public void aggregate()
      {
        if (predicate.matches()) {
          final Long v = selector.get();
          if (v != null) {
            holder.add(v);
          }
        }
      }
    };
  }

  public static Aggregator create(final ObjectColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return Aggregators.noopAggregator();
    }
    return new VarianceAggregator()
    {
      @Override
      public void aggregate()
      {
        if (predicate.matches()) {
          VarianceAggregatorCollector.combineValues(holder, selector.get());
        }
      }
    };
  }
}
