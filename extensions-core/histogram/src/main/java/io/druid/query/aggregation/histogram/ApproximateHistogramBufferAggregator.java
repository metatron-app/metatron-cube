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

package io.druid.query.aggregation.histogram;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;

public class ApproximateHistogramBufferAggregator implements BufferAggregator
{
  private final FloatColumnSelector selector;
  private final int resolution;
  private final float lowerLimit;
  private final float upperLimit;
  private final ValueMatcher predicate;

  public ApproximateHistogramBufferAggregator(
      FloatColumnSelector selector,
      int resolution,
      float lowerLimit,
      float upperLimit,
      ValueMatcher predicate
  )
  {
    this.selector = selector;
    this.resolution = resolution;
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.predicate = predicate;
  }

  @Override
  public void init(ByteBuffer buf, int position0, int position1)
  {
    buf.position(position1);
    buf.putInt(resolution);
    buf.putInt(0); //initial binCount
    for (int i = 0; i < resolution; ++i) {
      buf.putFloat(0f);
    }
    for (int i = 0; i < resolution; ++i) {
      buf.putLong(0L);
    }

    // min
    buf.putFloat(Float.POSITIVE_INFINITY);
    // max
    buf.putFloat(Float.NEGATIVE_INFINITY);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position0, int position1)
  {
    if (predicate.matches()) {
      final Float value = selector.get();
      if (value != null) {
        buf.position(position1);
        ApproximateHistogram h0 = new ApproximateHistogram().fromBytesDense(buf);
        h0.offer(value);

        buf.position(position1);
        h0.toBytesDense(buf);
      }
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position0, int position1)
  {
    buf.position(position1);
    return new ApproximateHistogram().fromBytes(buf);
  }
}
