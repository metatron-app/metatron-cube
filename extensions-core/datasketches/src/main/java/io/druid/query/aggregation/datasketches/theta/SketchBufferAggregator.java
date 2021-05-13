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

package io.druid.query.aggregation.datasketches.theta;

import com.google.common.collect.Lists;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.SetOperationBuilder;
import com.yahoo.sketches.theta.Union;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public abstract class SketchBufferAggregator extends Aggregators.BufferMapping<Union> implements BufferAggregator
{
  private final int nomEntries;
  private final int maxIntermediateSize;

  private final List<WritableMemory> nms = Lists.newArrayList();

  public SketchBufferAggregator(int nomEntries, int maxIntermediateSize)
  {
    this.nomEntries = nomEntries;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  @Override
  public void init(ByteBuffer buf, int position0, int position1)
  {
    SetOperationBuilder builder = SetOperation.builder().setNominalEntries(nomEntries);
    WritableMemory mem = asMemory(buf, position0).writableRegion(position1, maxIntermediateSize);
    put(position0, position1, (Union) builder.build(Family.UNION, mem));
  }

  private WritableMemory asMemory(ByteBuffer buf, int position0)
  {
    WritableMemory nm = position0 < nms.size() ? nms.get(position0) : null;
    if (nm == null) {
      for (int i = nms.size(); i <= position0; i++) {
        nms.add(null);
      }
      nms.set(position0, nm = WritableMemory.wrap(buf, ByteOrder.nativeOrder()));
    }
    return nm;
  }

  @Override
  public Object get(ByteBuffer buf, int position0, int position1)
  {
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return get(position0, position1).getResult(true, null);
  }

  @Override
  public void clear(boolean close)
  {
    nms.clear();
    mapping.clear();
  }

  public static SketchBufferAggregator create(DimensionSelector selector, int nomEntries, int maxIntermediateSize)
  {
    if (selector instanceof DimensionSelector.WithRawAccess) {
      return new SketchBufferAggregator(nomEntries, maxIntermediateSize)
      {
        final DimensionSelector.WithRawAccess rawAccess = (DimensionSelector.WithRawAccess) selector;

        @Override
        public void aggregate(ByteBuffer buf, int position0, int position1)
        {
          final IndexedInts indexed = rawAccess.getRow();
          final int length = indexed.size();
          if (length == 1) {
            Union union = get(position0, position1);
            union.update(rawAccess.lookupRaw(indexed.get(0)));
          } else if (length > 1) {
            Union union = get(position0, position1);
            for (int i = 0; i < length; i++) {
              union.update(rawAccess.lookupRaw(indexed.get(i)));
            }
          }
        }
      };
    } else {
      return new SketchBufferAggregator(nomEntries, maxIntermediateSize)
      {
        @Override
        public void aggregate(ByteBuffer buf, int position0, int position1)
        {
          final IndexedInts indexed = selector.getRow();
          final int length = indexed.size();
          if (length == 1) {
            Union union = get(position0, position1);
            union.update((String) selector.lookupName(indexed.get(0)));
          } else if (length > 1) {
            Union union = get(position0, position1);
            for (int i = 0; i < length; i++) {
              union.update((String) selector.lookupName(indexed.get(i)));
            }
          }
        }
      };
    }
  }

  public static SketchBufferAggregator create(final ObjectColumnSelector selector, int nomEntries, int maxIntermediateSize)
  {
    return new SketchBufferAggregator(nomEntries, maxIntermediateSize)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position0, int position1)
      {
        Object update = selector.get();
        if (update != null) {
          SketchAggregator.updateUnion(get(position0, position1), update);
        }
      }
    };
  }
}
