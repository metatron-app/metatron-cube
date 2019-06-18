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

package io.druid.query.aggregation.covariance;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.ObjectColumnSelector;

/**
 */
public abstract class CovarianceAggregator extends Aggregator.Abstract
{
  protected final CovarianceAggregatorCollector holder = new CovarianceAggregatorCollector();

  @Override
  public void reset()
  {
    holder.reset();
  }

  @Override
  public Object get()
  {
    return holder.duplicate();
  }

  public static Aggregator create(
      final DoubleColumnSelector selector1,
      final DoubleColumnSelector selector2,
      final ValueMatcher predicate
  )
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new CovarianceAggregator()
      {
        @Override
        public void aggregate()
        {
          final Double v1 = selector1.get();
          final Double v2 = selector2.get();
          if (v1 != null && v2 != null) {
            holder.add(v1, v2);
          }
        }
      };
    } else {
      return new CovarianceAggregator()
      {
        @Override
        public void aggregate()
        {
          if (predicate.matches()) {
            final Double v1 = selector1.get();
            final Double v2 = selector2.get();
            if (v1 != null && v2 != null) {
              holder.add(v1, v2);
            }
          }
        }
      };
    }
  }

  public static Aggregator create(final ObjectColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return Aggregators.noopAggregator();
    }
    return new CovarianceAggregator()
    {
      @Override
      public void aggregate()
      {
        if (predicate.matches()) {
          io.druid.query.aggregation.covariance.CovarianceAggregatorCollector.combineValues(holder, selector.get());
        }
      }
    };
  }
}
