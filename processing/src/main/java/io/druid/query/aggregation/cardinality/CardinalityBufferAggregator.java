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

package io.druid.query.aggregation.cardinality;

import io.druid.data.input.BytesOutputStream;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.HashAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.List;

public class CardinalityBufferAggregator extends BufferAggregator.Abstract
{
  private final List<DimensionSelector> selectorList;
  private final ValueMatcher predicate;
  private final int[][] groupings;
  private final boolean byRow;
  private final BytesOutputStream buffer = new BytesOutputStream();

  private static final byte[] EMPTY_BYTES = HyperLogLogCollector.makeEmptyVersionedByteArray();

  public CardinalityBufferAggregator(
      List<DimensionSelector> selectorList,
      ValueMatcher predicate,
      int[][] groupings,
      boolean byRow
  )
  {
    this.selectorList = selectorList;
    this.predicate = predicate;
    this.groupings = groupings;
    this.byRow = byRow;
  }

  public CardinalityBufferAggregator(List<DimensionSelector> selectorList, boolean byRow)
  {
    this(selectorList, ValueMatcher.TRUE, null, byRow);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    final ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.put(EMPTY_BYTES);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (predicate.matches()) {
      final int oldPosition = buf.position();
      final int oldLimit = buf.limit();
      buf.limit(position + HyperLogLogCollector.getLatestNumBytesForDenseStorage());
      buf.position(position);
      try {
        final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(buf);
        if (byRow) {
          HashAggregator.hashRow(selectorList, groupings, collector, buffer);
        } else {
          HashAggregator.hashValues(selectorList, collector, buffer);
        }
      }
      finally {
        buf.limit(oldLimit);
        buf.position(oldPosition);
      }
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer dataCopyBuffer = ByteBuffer.allocate(HyperLogLogCollector.getLatestNumBytesForDenseStorage());
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.get(dataCopyBuffer.array());
    return HyperLogLogCollector.makeCollector(dataCopyBuffer);
  }
}
