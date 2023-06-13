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

import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableFloat;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.OptionalDouble;

/**
 */
public abstract class DoubleMaxAggregator implements Aggregator.FromMutableDouble, Aggregator.DoubleStreaming
{
  static final Comparator COMPARATOR = DoubleSumAggregator.COMPARATOR;

  public static DoubleMaxAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    return new DoubleMaxAggregator()
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
      public MutableDouble aggregate(final MutableDouble current)
      {
        if (predicate.matches() && selector.getFloat(handover)) {
          if (current == null) {
            return new MutableDouble(handover.floatValue());
          }
          current.setValue(Math.max(current.floatValue(), handover.floatValue()));
        }
        return current;
      }
    };
  }

  public static DoubleMaxAggregator create(final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    return new DoubleMaxAggregator()
    {
      private final MutableDouble handover = new MutableDouble();

      @Override
      public boolean supports()
      {
        return predicate == ValueMatcher.TRUE && selector instanceof DoubleColumnSelector.Scannable;
      }

      @Override
      public Object aggregate(IntIterator iterator)
      {
        OptionalDouble max = ((DoubleColumnSelector.Scannable) selector).stream(iterator).max();
        return max.isPresent() ? max.getAsDouble() : null;
      }

      @Override
      public MutableDouble aggregate(final MutableDouble current)
      {
        if (predicate.matches() && selector.getDouble(handover)) {
          if (current == null) {
            return new MutableDouble(handover.doubleValue());
          }
          current.setValue(Math.max(current.doubleValue(), handover.doubleValue()));
        }
        return current;
      }
    };
  }

  public static Vectorized<Double[]> vectorize(DoubleColumnSelector.Scannable selector)
  {
    return new Vectorized<Double[]>()
    {
      @Override
      public void close() throws IOException
      {
        selector.close();
      }

      @Override
      public Double[] init(int length)
      {
        return new Double[length];
      }

      @Override
      public void aggregate(IntIterator iterator, Double[] vector, Int2IntFunction offset)
      {
        selector.consume(iterator, (i, x) -> {
          int ix = offset.applyAsInt(i);
          vector[ix] = vector[ix] == null ? x : Math.max(vector[ix], x);
        });
      }

      @Override
      public Object get(Double[] vector, int offset)
      {
        return vector[offset];
      }
    };
  }
}
