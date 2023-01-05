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

import io.druid.math.expr.Expr;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.LongColumnSelector;
import org.apache.commons.lang.mutable.MutableLong;

import java.nio.ByteBuffer;

/**
 *
 */
public abstract class LongSumBufferAggregator implements BufferAggregator
{
  @Override
  public void init(ByteBuffer buf, int position0, int position1)
  {
    buf.putLong(position1, 0);
  }

  @Override
  public Object get(ByteBuffer buf, int position0, int position1)
  {
    return buf.getLong(position1);
  }

  public static LongSumBufferAggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    if (selector instanceof Expr.LongOptimized) {
      return new LongSumBufferAggregator()
      {
        private final Expr.LongOptimized optimized = (Expr.LongOptimized) selector;
        private final MutableLong handover = new MutableLong();

        @Override
        public void aggregate(ByteBuffer buf, int position0, int position1)
        {
          if (predicate.matches() && optimized.getLong(handover)) {
            _aggregate(buf, position1, handover.longValue());
          }
        }
      };
    }
    return new LongSumBufferAggregator()
    {
      private final MutableLong handover = new MutableLong();

      @Override
      public void aggregate(ByteBuffer buf, int position0, int position1)
      {
        if (predicate.matches() && selector.getLong(handover)) {
          _aggregate(buf, position1, handover.longValue());
        }
      }
    };
  }

  private static void _aggregate(final ByteBuffer buf, final int position, final long current)
  {
    buf.putLong(position, current + buf.getLong(position));
  }
}
