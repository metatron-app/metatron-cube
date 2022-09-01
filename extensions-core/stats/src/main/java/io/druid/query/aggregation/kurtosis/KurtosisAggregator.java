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

package io.druid.query.aggregation.kurtosis;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 */
public abstract class KurtosisAggregator implements Aggregator.Simple<KurtosisAggregatorCollector>
{
  public static Aggregator create(final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    return new KurtosisAggregator()
    {
      private final MutableDouble handover = new MutableDouble();

      @Override
      public KurtosisAggregatorCollector aggregate(KurtosisAggregatorCollector current)
      {
        if (predicate.matches() && selector.getDouble(handover)) {
          if (current == null) {
            current = new KurtosisAggregatorCollector();
          }
          return current.add(handover.doubleValue());
        }
        return current;
      }
    };
  }

  public static Aggregator create(final ObjectColumnSelector<KurtosisAggregatorCollector> selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return NULL;
    }
    return new KurtosisAggregator()
    {
      @Override
      public KurtosisAggregatorCollector aggregate(KurtosisAggregatorCollector current)
      {
        if (predicate.matches()) {
          return KurtosisAggregatorCollector.combineValues(current, selector.get());
        }
        return current;
      }
    };
  }
}
