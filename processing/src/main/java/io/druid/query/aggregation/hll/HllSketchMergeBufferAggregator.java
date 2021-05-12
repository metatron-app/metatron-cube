/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.query.aggregation.hll;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This aggregator merges existing sketches.
 * The input column must contain {@link HllSketch}
 * @author Alexander Saydakov
 */
public class HllSketchMergeBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector<HllSketch> selector;
  private final int lgK;
  private final TgtHllType tgtHllType;
  private final int size;

  public HllSketchMergeBufferAggregator(
      final ObjectColumnSelector<HllSketch> selector,
      final int lgK,
      final TgtHllType tgtHllType,
      final int size
  )
  {
    this.selector = selector;
    this.lgK = lgK;
    this.tgtHllType = tgtHllType;
    this.size = size;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    // Not necessary to keep the constructed object since it is cheap to reconstruct by wrapping the memory.
    // The objects are not cached as in BuildBufferAggregator since they never exceed the max size and never move.
    // So it is easier to reconstruct them by wrapping memory then to keep position-to-object mappings. 
    new Union(lgK, asWritableMemory(buf, position));
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    final HllSketch sketch = selector.get();
    if (sketch != null) {
      toUnion(buf, position).update(sketch);
    }
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return toUnion(buf, position).getResult(tgtHllType);
  }

  private Union toUnion(final ByteBuffer buf, final int position)
  {
    return Union.writableWrap(asWritableMemory(buf, position));
  }

  private WritableMemory asWritableMemory(final ByteBuffer buf, final int position)
  {
    return WritableMemory.wrap(buf, ByteOrder.nativeOrder()).writableRegion(position, size);
  }
}
