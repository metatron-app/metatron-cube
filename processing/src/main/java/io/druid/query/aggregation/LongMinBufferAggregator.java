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
public abstract class LongMinBufferAggregator implements BufferAggregator
{
  public static LongMinBufferAggregator create(final LongColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new LongMinBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          final Long v2 = selector.get();
          if (v2 != null) {
            final long v1 = buf.getLong(position);
            if (Long.compare(v1, v2) >= 0) {
              buf.putLong(position, v2);
            }
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
            final Long v2 = selector.get();
            if (v2 != null) {
              final long v1 = buf.getLong(position);
              if (Long.compare(v1, v2) >= 0) {
                buf.putLong(position, v2);
              }
            }
          }
        }
      };
    }
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MAX_VALUE);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public Float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getLong(position);
  }

  @Override
  public Double getDouble(ByteBuffer buf, int position)
  {
    return (double) buf.getLong(position);
  }

  @Override
  public Long getLong(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
