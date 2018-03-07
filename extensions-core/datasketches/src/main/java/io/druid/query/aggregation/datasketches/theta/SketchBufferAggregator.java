/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.theta;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public abstract class SketchBufferAggregator implements BufferAggregator
{
  private final int size;
  private final int maxIntermediateSize;

  private NativeMemory nm;
  private final Map<Integer, Union> unions = new HashMap<>(); //position in BB -> Union Object

  public SketchBufferAggregator(int size, int maxIntermediateSize)
  {
    this.size = size;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    if (nm == null) {
      nm = new NativeMemory(buf);
    }

    Memory mem = new MemoryRegion(nm, position, maxIntermediateSize);
    unions.put(position, (Union) SetOperation.builder().initMemory(mem).build(size, Family.UNION));
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return getUnion(buf, position).getResult(true, null);
  }

  //Note that this is not threadsafe and I don't think it needs to be
  protected final Union getUnion(ByteBuffer buf, int position)
  {
    Union union = unions.get(position);
    if (union == null) {
      Memory mem = new MemoryRegion(nm, position, maxIntermediateSize);
      union = (Union) SetOperation.wrap(mem);
      unions.put(position, union);
    }
    return union;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    unions.clear();
  }

  public static SketchBufferAggregator create(final DimensionSelector selector, int size, int maxIntermediateSize)
  {
    if (selector instanceof DimensionSelector.WithRawAccess) {
      final DimensionSelector.WithRawAccess rawAccess = (DimensionSelector.WithRawAccess)selector;
      return new SketchBufferAggregator(size, maxIntermediateSize)
      {
        @Override
        public void aggregate(ByteBuffer buf, int position)
        {
          final IndexedInts indexed = selector.getRow();
          final int length = indexed.size();
          if (length == 1) {
            Union union = getUnion(buf, position);
            union.update(rawAccess.lookupRaw(indexed.get(0)));
          } else if (length > 1) {
            Union union = getUnion(buf, position);
            for (int i = 0; i < length; i++) {
              union.update(rawAccess.lookupRaw(indexed.get(i)));
            }
          }
        }
      };
    } else {
      return new SketchBufferAggregator(size, maxIntermediateSize)
      {
        @Override
        public void aggregate(ByteBuffer buf, int position)
        {
          final IndexedInts indexed = selector.getRow();
          final int length = indexed.size();
          if (length == 1) {
            Union union = getUnion(buf, position);
            union.update((String) selector.lookupName(indexed.get(0)));
          } else if (length > 1) {
            for (int i = 0; i < length; i++) {
              Union union = getUnion(buf, position);
              union.update((String) selector.lookupName(indexed.get(i)));
            }
          }
        }
      };
    }
  }

  public static SketchBufferAggregator create(final ObjectColumnSelector selector, int size, int maxIntermediateSize)
  {
    return new SketchBufferAggregator(size, maxIntermediateSize)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        Object update = selector.get();
        if (update == null) {
          return;
        }

        Union union = getUnion(buf, position);
        SketchAggregator.updateUnion(union, update);
      }
    };
  }
}
