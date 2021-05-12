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

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class SketchBufferAggregator implements BufferAggregator
{
  private final int nomEntries;
  private final int maxIntermediateSize;

  private final Int2ObjectMap<Union> unions = new Int2ObjectOpenHashMap<>(); //position in BB -> Union Object

  public SketchBufferAggregator(int nomEntries, int maxIntermediateSize)
  {
    this.nomEntries = nomEntries;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    WritableMemory mem = WritableMemory.wrap(buf, ByteOrder.nativeOrder()).writableRegion(position, maxIntermediateSize);
    unions.put(position, (Union) SetOperation.builder().setNominalEntries(nomEntries).build(Family.UNION, mem));
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return getUnion(position).getResult(true, null);
  }

  //Note that this is not threadsafe and I don't think it needs to be
  protected final Union getUnion(int position)
  {
    return unions.get(position);
  }

  @Override
  public void clear(boolean close)
  {
    unions.clear();
  }

  public static SketchBufferAggregator create(DimensionSelector selector, int nomEntries, int maxIntermediateSize)
  {
    if (selector instanceof DimensionSelector.WithRawAccess) {
      return new SketchBufferAggregator(nomEntries, maxIntermediateSize)
      {
        final DimensionSelector.WithRawAccess rawAccess = (DimensionSelector.WithRawAccess) selector;

        @Override
        public void aggregate(ByteBuffer buf, int position)
        {
          final IndexedInts indexed = rawAccess.getRow();
          final int length = indexed.size();
          if (length == 1) {
            Union union = getUnion(position);
            union.update(rawAccess.lookupRaw(indexed.get(0)));
          } else if (length > 1) {
            Union union = getUnion(position);
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
        public void aggregate(ByteBuffer buf, int position)
        {
          final IndexedInts indexed = selector.getRow();
          final int length = indexed.size();
          if (length == 1) {
            Union union = getUnion(position);
            union.update((String) selector.lookupName(indexed.get(0)));
          } else if (length > 1) {
            Union union = getUnion(position);
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
      public void aggregate(ByteBuffer buf, int position)
      {
        Object update = selector.get();
        if (update == null) {
          return;
        }

        Union union = getUnion(position);
        SketchAggregator.updateUnion(union, update);
      }
    };
  }
}
