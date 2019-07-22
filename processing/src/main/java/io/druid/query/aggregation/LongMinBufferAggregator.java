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

import java.nio.ByteBuffer;

/**
 */
public abstract class LongMinBufferAggregator extends BufferAggregator.NullSupport
{
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return isNull(buf, position) ? null : buf.getLong(Byte.BYTES + position);
  }

  public static LongMinBufferAggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new LongMinBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          final Long current = selector.get();
          if (current != null) {
            _aggregate(buf, position, current);
          }
        }
      };
    } else {
      return new LongMinBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          if (predicate.matches()) {
            final Long current = selector.get();
            if (current != null) {
              _aggregate(buf, position, current);
            }
          }
        }
      };
    }
  }

  private static void _aggregate(final ByteBuffer buf, final int position, final long current)
  {
    if (isNull(buf, position)) {
      buf.put(position, NOT_NULL);
      buf.putLong(Byte.BYTES + position, current);
    } else {
      final long prev = buf.getLong(Byte.BYTES + position);
      if (Long.compare(current, prev) < 0) {
        buf.putLong(Byte.BYTES + position, current);
      }
    }
  }
}
