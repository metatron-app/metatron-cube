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

import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class HistogramBufferAggregator implements BufferAggregator
{
  private final FloatColumnSelector selector;
  private final float[] breaks;
  private final int minOffset;
  private final int maxOffset;

  public HistogramBufferAggregator(FloatColumnSelector selector, float[] breaks)
  {
    this.selector = selector;
    this.breaks   = breaks;
    this.minOffset = Long.BYTES * (breaks.length + 1);
    this.maxOffset = this.minOffset + Float.BYTES;
  }

  @Override
  public void init(ByteBuffer buf, int position0, int position1)
  {
    buf.position(position1);

    final long[] bins = new long[breaks.length + 1];
    buf.asLongBuffer().put(bins);
    buf.putFloat(position1 + minOffset, Float.MAX_VALUE);
    buf.putFloat(position1 + maxOffset, Float.MIN_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position0, int position1)
  {
    final Float v = selector.get();
    if (v == null) {
      return;
    }
    final float value = v;
    final int minPos = position1 + minOffset;
    final int maxPos = position1 + maxOffset;

    if(value < buf.getFloat(minPos)) buf.putFloat(minPos, value);
    if(value > buf.getFloat(maxPos)) buf.putFloat(maxPos, value);

    int index = Arrays.binarySearch(breaks, value);
    index = (index >= 0) ? index : -(index + 1);

    final int offset = position1 + (index * Long.BYTES);
    final long count = buf.getLong(offset);
    buf.putLong(offset, count + 1);
  }

  @Override
  public Object get(ByteBuffer buf, int position0, int position1)
  {
    final long[] bins = new long[breaks.length + 1];
    buf.position(position1);
    buf.asLongBuffer().get(bins);

    float min = buf.getFloat(position1 + minOffset);
    float max = buf.getFloat(position1 + maxOffset);
    return new Histogram(breaks, bins, min, max);
  }
}
