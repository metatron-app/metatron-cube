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
 *
 */
public abstract class DoubleSumBufferAggregator implements BufferAggregator
{
  @Override
  public void init(ByteBuffer buf, int position0, int position1)
  {
    buf.putDouble(position1, 0);
  }

  @Override
  public Object get(ByteBuffer buf, int position0, int position1)
  {
    return buf.getDouble(position1);
  }

  public static DoubleSumBufferAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    return new DoubleSumBufferAggregator()
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

  public static DoubleSumBufferAggregator create(final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    return new DoubleSumBufferAggregator()
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
    buf.putDouble(position, current + buf.getDouble(position));
  }
}
