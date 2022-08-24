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
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableFloat;

import java.nio.ByteBuffer;

/**
 */
public abstract class DoubleMinBufferAggregator extends BufferAggregator.NullSupport
{
  @Override
  public Object get(ByteBuffer buf, int position0, int position1)
  {
    return isNull(buf, position1) ? null : buf.getDouble(Byte.BYTES + position1);
  }

  public static DoubleMinBufferAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    return new DoubleMinBufferAggregator()
    {
      private final MutableFloat handover = new MutableFloat();

      @Override
      public void aggregate(ByteBuffer buf, int position0, int position1)
      {
        if (predicate.matches() && selector.getFloat(handover)) {
          _aggregate(buf, position1, handover.floatValue());
        }
      }
    };
  }

  public static DoubleMinBufferAggregator create(final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    return new DoubleMinBufferAggregator()
    {
      private final MutableDouble handover = new MutableDouble();

      @Override
      public void aggregate(ByteBuffer buf, int position0, int position1)
      {
        if (predicate.matches() && selector.getDouble(handover)) {
          _aggregate(buf, position1, handover.doubleValue());
        }
      }
    };
  }

  private static void _aggregate(final ByteBuffer buf, final int position, final double current)
  {
    if (isNull(buf, position)) {
      buf.put(position, NOT_NULL);
      buf.putDouble(Byte.BYTES + position, current);
    } else {
      final double prev = buf.getDouble(Byte.BYTES + position);
      if (Double.compare(current, prev) < 0) {
        buf.putDouble(Byte.BYTES + position, current);
      }
    }
  }
}
