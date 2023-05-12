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
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.math.expr.Expr;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.LongColumnSelector;
import org.apache.commons.lang.mutable.MutableLong;
import org.roaringbitmap.IntIterator;

import java.util.Comparator;

/**
 */
public abstract class LongSumAggregator implements Aggregator.FromMutableLong
{
  static final Comparator COMPARATOR = Comparators.NULL_FIRST(
      (o1, o2) -> Long.compare(((Number) o1).longValue(), ((Number) o2).longValue())
  );

  static final BinaryFn.Identical<Number> COMBINER = (lhs, rhs) -> lhs.longValue() + rhs.longValue();

  static abstract class ScanSupport extends LongSumAggregator implements Aggregator.LongScannable { }

  @Override
  public Long get(MutableLong current)
  {
    return current == null ? 0L : current.longValue();
  }

  @Override
  public boolean getLong(MutableLong current, MutableLong handover)
  {
    handover.setValue(current == null ? 0L : current.longValue());
    return true;
  }

  public static LongSumAggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector instanceof Expr.LongOptimized) {
      return new LongSumAggregator()
      {
        private final MutableLong handover = new MutableLong();
        private final Expr.LongOptimized optimized = (Expr.LongOptimized) selector;

        @Override
        public MutableLong aggregate(final MutableLong current)
        {
          if (predicate.matches() && optimized.getLong(handover)) {
            if (current == null) {
              return new MutableLong(handover.longValue());
            }
            current.add(handover.longValue());
          }
          return current;
        }
      };
    }
    return new LongSumAggregator.ScanSupport()
    {
      private final MutableLong handover = new MutableLong();

      @Override
      public boolean supports()
      {
        return predicate == ValueMatcher.TRUE && selector instanceof LongColumnSelector.Scannable;
      }

      @Override
      public Object aggregate(IntIterator iterator)
      {
        return ((LongColumnSelector.Scannable) selector).stream(iterator).sum();
      }

      @Override
      public MutableLong aggregate(final MutableLong current)
      {
        if (predicate.matches() && selector.getLong(handover)) {
          if (current == null) {
            return new MutableLong(handover.longValue());
          }
          current.add(handover.longValue());
        }
        return current;
      }
    };
  }
}
