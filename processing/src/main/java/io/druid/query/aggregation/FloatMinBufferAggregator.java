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
import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public abstract class FloatMinBufferAggregator implements BufferAggregator
{
  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putFloat(position, Float.POSITIVE_INFINITY);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getFloat(position);
  }

  @Override
  public Float getFloat(ByteBuffer buf, int position)
  {
    return buf.getFloat(position);
  }

  @Override
  public Double getDouble(ByteBuffer buf, int position)
  {
    return (double) buf.getFloat(position);
  }

  @Override
  public Long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getFloat(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  public static FloatMinBufferAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new FloatMinBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          final Float v2 = selector.get();
          if (v2 != null) {
            final float v1 = buf.getFloat(position);
            if (Float.compare(v1, v2) >= 0) {
              buf.putFloat(position, v2);
            }
          }
        }
      };
    } else {
      return new FloatMinBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          if (predicate.matches()) {
            final Float v2 = selector.get();
            if (v2 != null) {
              final float v1 = buf.getFloat(position);
              if (Float.compare(v1, v2) >= 0) {
                buf.putFloat(position, v2);
              }
            }
          }
        }
      };
    }
  }
}
