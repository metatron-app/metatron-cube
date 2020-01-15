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

package io.druid.query.aggregation.bloomfilter;

import io.druid.common.guava.BytesRef;
import io.druid.data.input.BytesOutputStream;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.HashAggregator;
import io.druid.query.aggregation.HashCollector;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class BloomFilterBufferAggregator extends BufferAggregator.Abstract
{
  private final int maxNumEntries;

  protected BloomFilterBufferAggregator(int maxNumEntries) {this.maxNumEntries = maxNumEntries;}

  public static BloomFilterBufferAggregator iterator(
      final List<DimensionSelector> selectorList,
      final ValueMatcher predicate,
      final int[][] groupings,
      final boolean byRow,
      final int maxNumEntries
  )
  {
    return new BloomFilterBufferAggregator(maxNumEntries)
    {
      private final BytesOutputStream buffer = new BytesOutputStream();

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        if (predicate.matches()) {
          final int oldPosition = buf.position();
          buf.position(position);
          try {
            BloomFilterCollector collector = new BloomFilterCollector(buf);
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

  @Override
  public void init(ByteBuffer buf, int position)
  {
    BloomKFilter.init(buf, position, maxNumEntries);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return BloomKFilter.deserialize(buf, position);
  }

  private static class BloomFilterCollector implements HashCollector
  {
    private final ByteBuffer byteBuffer;

    private BloomFilterCollector(ByteBuffer byteBuffer) {this.byteBuffer = byteBuffer;}

    @Override
    public void collect(Object[] values, BytesRef bytes)
    {
      BloomKFilter.collect(byteBuffer, values, bytes);
    }
  }
}
