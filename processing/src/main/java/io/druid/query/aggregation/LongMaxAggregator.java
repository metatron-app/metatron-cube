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

import io.druid.query.aggregation.AggregatorFactory.Combiner;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.LongColumnSelector;
import org.apache.commons.lang.mutable.MutableLong;

import java.util.Comparator;

/**
 */
public abstract class LongMaxAggregator implements Aggregator<MutableLong>
{
  static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

  static final Combiner<Number> COMBINER = new Combiner.Abstract<Number>()
  {
    @Override
    protected final Number _combine(Number lhs, Number rhs)
    {
      return Math.max(lhs.longValue(), rhs.longValue());
    }
  };

  @Override
  public Long get(MutableLong current)
  {
    return current == null ? null : current.longValue();
  }

  public static LongMaxAggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new LongMaxAggregator()
      {
        @Override
        public MutableLong aggregate(final MutableLong current)
        {
          final Long value = selector.get();
          if (value == null) {
            return current;
          }
          if (current == null) {
            return new MutableLong(value);
          }
          current.setValue(Math.max(current.longValue(), value));
          return current;
        }
      };
    } else {
      return new LongMaxAggregator()
      {
        @Override
        public final MutableLong aggregate(MutableLong current)
        {
          if (predicate.matches()) {
            final Long value = selector.get();
            if (value == null) {
              return current;
            }
            if (current == null) {
              return new MutableLong(value);
            }
            current.setValue(Math.max(current.longValue(), value));
          }
          return current;
        }
      };
    }
  }
}
