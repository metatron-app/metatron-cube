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

package io.druid.query.aggregation.countmin;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.HashBufferAggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.List;

public class CountMinBufferAggregator extends HashBufferAggregator.ScanSupport<CountMinSketch>
{
  private final int width;
  private final int depth;

  public CountMinBufferAggregator(
      final ValueMatcher predicate,
      final List<DimensionSelector> selectorList,
      final int[][] groupings,
      final boolean byRow,
      final int width,
      final int depth
  )
  {
    super(predicate, selectorList, groupings, byRow);
    this.width = width;
    this.depth = depth;
  }

  public static BufferAggregator combiner(
      final ObjectColumnSelector<CountMinSketch> selector,
      final int width,
      final int depth
  )
  {
    return new BufferAggregator()
    {
      @Override
      public void init(ByteBuffer buf, int position0, int position1)
      {
        buf.position(position1);
        buf.putInt(width);
        buf.putInt(depth);
      }

      @Override
      public void aggregate(ByteBuffer buf, int position0, int position1)
      {
        final CountMinSketch other = selector.get();
        if (other != null) {
          CountMinSketch collector = CountMinSketch.fromBytes(buf, position1).merge(other);
          buf.position(position1);
          buf.put(collector.toBytes());
        }
      }

      @Override
      public Object get(ByteBuffer buf, int position0, int position1)
      {
        return CountMinSketch.fromBytes(buf, position1);
      }
    };
  }

  @Override
  public void init(ByteBuffer buf, int position0, int position1)
  {
    buf.position(position1);
    buf.putInt(width);
    buf.putInt(depth);
  }

  @Override
  protected CountMinSketch toCollector(ByteBuffer buf, int position)
  {
    return CountMinSketch.fromBytes(buf, position);
  }

  @Override
  public Object get(ByteBuffer buf, int position0, int position1)
  {
    return CountMinSketch.fromBytes(buf, position1);
  }
}
