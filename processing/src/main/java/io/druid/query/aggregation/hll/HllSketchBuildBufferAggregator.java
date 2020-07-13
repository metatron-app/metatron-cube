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
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This aggregator builds sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 * @author Alexander Saydakov
 */
public class HllSketchBuildBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector<Object> selector;
  private final int lgK;
  private final TgtHllType tgtHllType;
  private final int size;

  public HllSketchBuildBufferAggregator(
      final ObjectColumnSelector<Object> selector,
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
    new HllSketch(lgK, tgtHllType, asWritableMemory(buf, position));
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    final Object value = selector.get();
    if (value != null) {
      HllSketchBuildAggregator.updateSketch(toSketch(buf, position), value);
    }
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return toSketch(buf, position);
  }

  private HllSketch toSketch(final ByteBuffer buf, final int position)
  {
    return HllSketch.writableWrap(asWritableMemory(buf, position));
  }

  private WritableMemory asWritableMemory(final ByteBuffer buf, final int position)
  {
    return WritableMemory.wrap(buf, ByteOrder.LITTLE_ENDIAN).writableRegion(position, size);
  }
}
