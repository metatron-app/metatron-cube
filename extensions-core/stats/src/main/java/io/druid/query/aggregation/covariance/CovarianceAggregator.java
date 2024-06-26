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
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 */
public abstract class CovarianceAggregator implements Aggregator.Simple<CovarianceAggregatorCollector>
{
  public static Aggregator create(
      final DoubleColumnSelector selector1,
      final DoubleColumnSelector selector2,
      final ValueMatcher predicate
  )
  {
    return new CovarianceAggregator()
    {
      private final MutableDouble handover1 = new MutableDouble();
      private final MutableDouble handover2 = new MutableDouble();

      @Override
      public CovarianceAggregatorCollector aggregate(CovarianceAggregatorCollector current)
      {
        if (predicate.matches() && selector1.getDouble(handover1) && selector2.getDouble(handover2)) {
          if (current == null) {
            current = new CovarianceAggregatorCollector();
          }
          current.add(handover1.doubleValue(), handover2.doubleValue());
        }
        return current;
      }
    };
  }

  public static Aggregator create(final ObjectColumnSelector<CovarianceAggregatorCollector> selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return NULL;
    }
    return new CovarianceAggregator()
    {
      @Override
      public CovarianceAggregatorCollector aggregate(CovarianceAggregatorCollector current)
      {
        if (predicate.matches()) {
          return CovarianceAggregatorCollector.combineValues(current, selector.get());
        }
        return current;
      }
    };
  }
}
