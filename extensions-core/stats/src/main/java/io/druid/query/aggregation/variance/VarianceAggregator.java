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

package io.druid.query.aggregation.variance;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableLong;

/**
 */
public abstract class VarianceAggregator implements Aggregator.Simple<VarianceAggregatorCollector>
{
  public static Aggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    return new VarianceAggregator()
    {
      private final MutableFloat handover = new MutableFloat();

      @Override
      public VarianceAggregatorCollector aggregate(VarianceAggregatorCollector current)
      {
        if (predicate.matches() && selector.getFloat(handover)) {
          if (current == null) {
            current = new VarianceAggregatorCollector();
          }
          current.add(handover.floatValue());
        }
        return current;
      }
    };
  }

  public static Aggregator create(final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    return new VarianceAggregator()
    {
      private final MutableDouble handover = new MutableDouble();

      @Override
      public VarianceAggregatorCollector aggregate(VarianceAggregatorCollector current)
      {
        if (predicate.matches() && selector.getDouble(handover)) {
          if (current == null) {
            current = new VarianceAggregatorCollector();
          }
          current.add(handover.doubleValue());
        }
        return current;
      }
    };
  }

  public static Aggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    return new VarianceAggregator()
    {
      private final MutableLong handover = new MutableLong();

      @Override
      public VarianceAggregatorCollector aggregate(VarianceAggregatorCollector current)
      {
        if (predicate.matches() && selector.getLong(handover)) {
          if (current == null) {
            current = new VarianceAggregatorCollector();
          }
          current.add(handover.longValue());
        }
        return current;
      }
    };
  }

  public static Aggregator create(final ObjectColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector == null) {
      return NULL;
    }
    return new VarianceAggregator()
    {
      @Override
      public VarianceAggregatorCollector aggregate(VarianceAggregatorCollector current)
      {
        if (predicate.matches()) {
          final Object v = selector.get();
          if (v != null) {
            if (current == null) {
              current = new VarianceAggregatorCollector();
            }
            return VarianceAggregatorCollector.combineValues(current, v);
          }
        }
        return current;
      }
    };
  }
}
