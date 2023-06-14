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
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableFloat;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.util.Comparator;

/**
 */
public abstract class DoubleSumAggregator implements Aggregator.FromMutableDouble
{
  static final Comparator COMPARATOR = Comparators.NULL_FIRST(
      (o1, o2) -> Double.compare(((Number) o1).doubleValue(), ((Number) o2).doubleValue())
  );

  static final BinaryFn.Identical<Number> COMBINER = (param1, param2) -> param1.doubleValue() + param2.doubleValue();

  static abstract class StreamingSupport extends DoubleSumAggregator implements DoubleStreaming { }

  @Override
  public Double get(MutableDouble current)
  {
    return current == null ? 0D : current.doubleValue();
  }

  @Override
  public boolean getDouble(MutableDouble current, MutableDouble handover)
  {
    handover.setValue(current == null ? 0D : current.doubleValue());
    return true;
  }

  public static Aggregator.FromMutableDouble create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector instanceof Expr.FloatOptimized) {
      return new DoubleSumAggregator()
      {
        private final MutableFloat handover = new MutableFloat();
        private final Expr.FloatOptimized optimized = (Expr.FloatOptimized) selector;

        @Override
        public MutableDouble aggregate(final MutableDouble current)
        {
          if (predicate.matches() && optimized.getFloat(handover)) {
            if (current == null) {
              return new MutableDouble(handover.floatValue());
            }
            current.add(handover.floatValue());
          }
          return current;
        }
      };
    }
    return new StreamingSupport()
    {
      private final MutableFloat handover = new MutableFloat();

      @Override
      public boolean supports()
      {
        return selector instanceof FloatColumnSelector.Scannable;
      }

      @Override
      public Object aggregate(IntIterator iterator)
      {
        return ((FloatColumnSelector.Scannable) selector).stream(iterator).sum();
      }

      @Override
      public MutableDouble aggregate(final MutableDouble current)
      {
        if (predicate.matches() && selector.getFloat(handover)) {
          if (current == null) {
            return new MutableDouble(handover.floatValue());
          }
          current.add(handover.floatValue());
        }
        return current;
      }
    };
  }

  public static DoubleSumAggregator create(final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector instanceof Expr.DoubleOptimized) {
      return new DoubleSumAggregator()
      {
        private final MutableDouble handover = new MutableDouble();
        private final Expr.DoubleOptimized optimized = (Expr.DoubleOptimized) selector;

        @Override
        public MutableDouble aggregate(final MutableDouble current)
        {
          if (predicate.matches() && optimized.getDouble(handover)) {
            if (current == null) {
              return new MutableDouble(handover.doubleValue());
            }
            current.add(handover.doubleValue());
          }
          return current;
        }
      };
    }
    return new StreamingSupport()
    {
      private final MutableDouble handover = new MutableDouble();

      @Override
      public boolean supports()
      {
        return selector instanceof DoubleColumnSelector.Scannable;
      }

      @Override
      public Object aggregate(IntIterator iterator)
      {
        return ((DoubleColumnSelector.Scannable) selector).stream(iterator).sum();
      }

      @Override
      public MutableDouble aggregate(final MutableDouble current)
      {
        if (predicate.matches() && selector.getDouble(handover)) {
          if (current == null) {
            return new MutableDouble(handover.doubleValue());
          }
          current.add(handover.doubleValue());
        }
        return current;
      }
    };
  }

  public static Vectorized<double[]> vectorize(DoubleColumnSelector.Scannable selector)
  {
    return new Vectorized<double[]>()
    {
      @Override
      public void close() throws IOException
      {
        selector.close();
      }

      @Override
      public double[] init(int length)
      {
        return new double[length];
      }

      @Override
      public void aggregate(IntIterator iterator, double[] vector, Int2IntFunction offset, int size)
      {
        selector.consume(iterator, (i, x) -> vector[offset.applyAsInt(i)] += x);
      }

      @Override
      public Object get(double[] vector, int offset)
      {
        return vector[offset];
      }
    };
  }
}
