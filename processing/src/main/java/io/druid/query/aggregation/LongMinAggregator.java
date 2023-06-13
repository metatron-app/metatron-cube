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
import io.druid.segment.LongColumnSelector;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import org.apache.commons.lang.mutable.MutableLong;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.OptionalLong;

/**
 *
 */
public abstract class LongMinAggregator implements Aggregator.FromMutableLong, Aggregator.LongStreaming
{
  static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

  public static LongMinAggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    return new LongMinAggregator()
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
        OptionalLong min = ((LongColumnSelector.Scannable) selector).stream(iterator).min();
        return min.isPresent() ? min.getAsLong() : null;
      }

      @Override
      public MutableLong aggregate(final MutableLong current)
      {
        if (predicate.matches() && selector.getLong(handover)) {
          if (current == null) {
            return new MutableLong(handover.longValue());
          }
          current.setValue(Math.min(current.longValue(), handover.longValue()));
        }
        return current;
      }
    };
  }

  public static Vectorized<MutableLong[]> vectorize(LongColumnSelector.Scannable selector)
  {
    return new Vectorized<MutableLong[]>()
    {
      @Override
      public void close() throws IOException
      {
        selector.close();
      }

      @Override
      public MutableLong[] init(int length)
      {
        return new MutableLong[length];
      }

      @Override
      public void aggregate(IntIterator iterator, MutableLong[] vector, Int2IntFunction offset)
      {
        selector.consume(iterator, (i, x) -> {
          int ix = offset.applyAsInt(i);
          if (vector[ix] == null) {
            vector[ix] = new MutableLong(x);
          } else {
            vector[ix].setValue(Math.min(vector[ix].longValue(), x));
          }
        });
      }

      @Override
      public Object get(MutableLong[] vector, int offset)
      {
        return vector[offset];
      }
    };
  }
}
