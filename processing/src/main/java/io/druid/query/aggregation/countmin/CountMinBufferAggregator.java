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

import io.druid.data.input.BytesOutputStream;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.HashAggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class CountMinBufferAggregator extends BufferAggregator.Abstract
{
  private final int width;
  private final int depth;

  protected CountMinBufferAggregator(int width, int depth)
  {
    this.width = width;
    this.depth = depth;
  }

  public static CountMinBufferAggregator iterator(
      final List<DimensionSelector> selectorList,
      final ValueMatcher predicate,
      final int[][] groupings,
      final boolean byRow,
      final int width,
      final int depth
  )
  {
    return new CountMinBufferAggregator(width, depth)
    {
      private final BytesOutputStream buffer = new BytesOutputStream();

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        if (predicate.matches()) {
          final int oldPosition = buf.position();
          buf.position(position);
          try {
            final CountMinSketch collector = CountMinSketch.fromBytes(buf);
            if (byRow) {
              HashAggregator.hashRow(selectorList, groupings, collector, buffer);
            } else {
              HashAggregator.hashValues(selectorList, collector, buffer);
            }
          }
          finally {
            buf.position(oldPosition);
          }
        }
      }
    };
  }

  public static BufferAggregator combiner(final ObjectColumnSelector<CountMinSketch> selector, int width, int depth)
  {
    return new CountMinBufferAggregator(width, depth)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        final int oldPosition = buf.position();
        buf.position(position);
        try {
          CountMinSketch collector = CountMinSketch.fromBytes(buf);
          buf.position(position);
          buf.put(collector.merge(selector.get()).toBytes());
        }
        finally {
          buf.position(oldPosition);
        }
      }
    };
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    final ByteBuffer duplicate = buf.duplicate();
    duplicate.position(position);
    buf.putInt(width);
    buf.putInt(depth);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    final ByteBuffer duplicate = buf.duplicate();
    duplicate.position(position);
    return CountMinSketch.fromBytes(duplicate);
  }
}
