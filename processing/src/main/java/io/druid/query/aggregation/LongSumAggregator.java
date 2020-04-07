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

import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.LongColumnSelector;
import org.apache.commons.lang.mutable.MutableLong;

import java.util.Comparator;

/**
 */
public abstract class LongSumAggregator implements Aggregator<MutableLong>
{
  static final Comparator COMPARATOR = new Ordering()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((Number) o).longValue(), ((Number) o1).longValue());
    }
  }.nullsFirst();

  static long combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).longValue() + ((Number) rhs).longValue();
  }

  @Override
  public Object get(MutableLong current)
  {
    return current == null ? 0L : current.longValue();
  }

  public static LongSumAggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new LongSumAggregator()
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
          current.add(value);
          return current;
        }
      };
    } else {
      return new LongSumAggregator()
      {
        @Override
        public MutableLong aggregate(final MutableLong current)
        {
          if (predicate.matches()) {
            final Long value = selector.get();
            if (value == null) {
              return current;
            }
            if (current == null) {
              return new MutableLong(value);
            }
            current.add(value);
          }
          return current;
        }
      };
    }
  }
}
