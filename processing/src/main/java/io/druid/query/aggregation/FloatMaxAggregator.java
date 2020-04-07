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
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.FloatColumnSelector;
import org.apache.commons.lang.mutable.MutableFloat;

import java.util.Comparator;

/**
 */
public abstract class FloatMaxAggregator implements Aggregator<MutableFloat>
{
  static final Comparator COMPARATOR = new Ordering()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Float.compare(((Number) o).floatValue(), ((Number) o1).floatValue());
    }
  }.nullsFirst();

  static float combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number) lhs).floatValue(), ((Number) rhs).floatValue());
  }

  @Override
  public Float get(MutableFloat current)
  {
    return current == null ? null : current.floatValue();
  }

  public static FloatMaxAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new FloatMaxAggregator()
      {
        @Override
        public MutableFloat aggregate(final MutableFloat current)
        {
          final Float value = selector.get();
          if (value == null) {
            return current;
          }
          if (current == null) {
            return new MutableFloat(value);
          }
          current.setValue(Math.max(current.floatValue(), value));
          return current;
        }
      };
    } else {
      return new FloatMaxAggregator()
      {
        @Override
        public final MutableFloat aggregate(MutableFloat current)
        {
          if (predicate.matches()) {
            final Float value = selector.get();
            if (value == null) {
              return current;
            }
            if (current == null) {
              return new MutableFloat(value);
            }
            current.setValue(Math.max(current.floatValue(), value));
          }
          return current;
        }
      };
    }
  }
}
