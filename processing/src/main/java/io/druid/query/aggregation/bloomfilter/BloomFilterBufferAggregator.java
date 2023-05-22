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

import io.druid.query.aggregation.HashBufferAggregator;
import io.druid.query.aggregation.HashCollector;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.List;

public class BloomFilterBufferAggregator extends HashBufferAggregator<BloomFilterBufferAggregator.Collector>
{
  private final int maxNumEntries;

  public BloomFilterBufferAggregator(
      final ValueMatcher predicate,
      final List<DimensionSelector> selectorList,
      final int[][] groupings,
      final boolean byRow,
      final int maxNumEntries
  )
  {
    super(predicate, selectorList, groupings, byRow);
    this.maxNumEntries = maxNumEntries;
  }

  @Override
  protected Class<Collector> collectorClass()
  {
    return Collector.class;
  }

  @Override
  protected Collector toCollector(ByteBuffer buf, int position)
  {
    return new Collector(buf, position);
  }

  @Override
  public void init(ByteBuffer buf, int position0, int position1)
  {
    BloomKFilter.init(buf, position1, maxNumEntries);
  }

  @Override
  public Object get(ByteBuffer buf, int position0, int position1)
  {
    return BloomKFilter.deserialize(buf, position1);
  }

  static class Collector implements HashCollector
  {
    private final ByteBuffer byteBuffer;
    private final int position;

    private Collector(ByteBuffer byteBuffer, int position)
    {
      this.byteBuffer = byteBuffer;
      this.position = position;
    }

    @Override
    public void collect(long hash64)
    {
      BloomKFilter.collect(byteBuffer, position, hash64);
    }
  }
}
