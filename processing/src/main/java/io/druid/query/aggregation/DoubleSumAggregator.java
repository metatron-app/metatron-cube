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
import com.google.common.primitives.Doubles;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import org.apache.commons.lang.mutable.MutableDouble;

import java.util.Comparator;

/**
 */
public abstract class DoubleSumAggregator extends Aggregator.Abstract<MutableDouble>
{
  static final Comparator COMPARATOR = new Ordering()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Doubles.compare(((Number) o).doubleValue(), ((Number) o1).doubleValue());
    }
  }.nullsFirst();

  static double combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
  }

  @Override
  public Object get(MutableDouble current)
  {
    return current == null ? 0D : current.doubleValue();
  }

  public static DoubleSumAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new DoubleSumAggregator()
      {
        @Override
        public MutableDouble aggregate(final MutableDouble current)
        {
          final Float value = selector.get();
          if (value == null) {
            return current;
          }
          if (current == null) {
            return new MutableDouble(value);
          }
          current.add(value);
          return current;
        }
      };
    } else {
      return new DoubleSumAggregator()
      {
        @Override
        public MutableDouble aggregate(final MutableDouble current)
        {
          if (predicate.matches()) {
            final Float value = selector.get();
            if (value == null) {
              return current;
            }
            if (current == null) {
              return new MutableDouble(value);
            }
            current.add(value);
          }
          return current;
        }
      };
    }
  }

  public static DoubleSumAggregator create(final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new DoubleSumAggregator()
      {
        @Override
        public MutableDouble aggregate(final MutableDouble current)
        {
          final Double value = selector.get();
          if (value == null) {
            return current;
          }
          if (current == null) {
            return new MutableDouble(value);
          }
          current.add(value);
          return current;
        }
      };
    } else {
      return new DoubleSumAggregator()
      {
        @Override
        public MutableDouble aggregate(final MutableDouble current)
        {
          if (predicate.matches()) {
            final Double value = selector.get();
            if (value == null) {
              return current;
            }
            if (current == null) {
              return new MutableDouble(value);
            }
            current.add(value);
          }
          return current;
        }
      };
    }
  }
}
