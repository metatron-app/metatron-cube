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

import io.druid.common.guava.Comparators;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.FloatColumnSelector;
import org.apache.commons.lang.mutable.MutableFloat;
import org.roaringbitmap.IntIterator;

import java.util.Comparator;
import java.util.OptionalDouble;

/**
 *
 */
public abstract class FloatMaxAggregator implements Aggregator.FromMutableFloat, Aggregator.FloatStreaming
{
  static final Comparator COMPARATOR = Comparators.NULL_FIRST(
      (Object o1, Object o2) -> Float.compare(((Number) o1).floatValue(), ((Number) o2).floatValue())
  );

  public static Aggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    return new FloatMaxAggregator()
    {
      private final MutableFloat handover = new MutableFloat();

      @Override
      public boolean supports()
      {
        return predicate == ValueMatcher.TRUE && selector instanceof FloatColumnSelector.Scannable;
      }

      @Override
      public Object aggregate(IntIterator iterator)
      {
        OptionalDouble max = ((FloatColumnSelector.Scannable) selector).stream(iterator).max();
        return max.isPresent() ? (float) max.getAsDouble() : null;
      }

      @Override
      public MutableFloat aggregate(final MutableFloat current)
      {
        if (predicate.matches() && selector.getFloat(handover)) {
          if (current == null) {
            return new MutableFloat(handover.floatValue());
          }
          current.setValue(Math.max(current.floatValue(), handover.floatValue()));
        }
        return current;
      }
    };
  }
}
