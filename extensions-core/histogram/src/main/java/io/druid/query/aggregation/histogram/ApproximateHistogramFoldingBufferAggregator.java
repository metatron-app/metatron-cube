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
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

public class ApproximateHistogramFoldingBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector<ApproximateHistogramHolder> selector;
  private final int resolution;
  private final float upperLimit;
  private final float lowerLimit;
  private final ValueMatcher predicate;

  private float[] tmpBufferP;
  private long[] tmpBufferB;

  public ApproximateHistogramFoldingBufferAggregator(
      ObjectColumnSelector<ApproximateHistogramHolder> selector,
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

    tmpBufferP = new float[resolution];
    tmpBufferB = new long[resolution];
  }

  @Override
  public void init(ByteBuffer buf, int position0, int position1)
  {
    if (predicate.matches()) {
      ApproximateHistogram h = new ApproximateHistogram(resolution, lowerLimit, upperLimit);
      buf.position(position1);
      // use dense storage for aggregation
      h.toBytesDense(buf);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int position0, int position1)
  {
    buf.position(position1);

    ApproximateHistogram h0 = new ApproximateHistogram().fromBytesDense(buf);
    h0.setLowerLimit(lowerLimit);
    h0.setUpperLimit(upperLimit);
    ApproximateHistogramHolder hNext = selector.get();
    h0.foldFast(hNext, tmpBufferP, tmpBufferB);

    buf.position(position1);
    h0.toBytesDense(buf);
  }

  @Override
  public Object get(ByteBuffer buf, int position0, int position1)
  {
    return new ApproximateHistogram().fromBytesDense((ByteBuffer) buf.position(position1));
  }
}
