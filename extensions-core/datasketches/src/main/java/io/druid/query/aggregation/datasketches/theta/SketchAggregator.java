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

import com.metamx.common.ISE;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;

import java.util.List;

public abstract class SketchAggregator implements Aggregator
{
  final Union union;

  public SketchAggregator(int size)
  {
    union = new SynchronizedUnion((Union) SetOperation.builder().build(size, Family.UNION));
  }

  @Override
  public void reset()
  {
    union.reset();
  }

  @Override
  public Object get()
  {
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return union.getResult(true, null);
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    union.reset();
  }

  static void updateUnion(Union union, Object update)
  {
    if (update instanceof Memory) {
      union.update((Memory) update);
    } else if (update instanceof Sketch) {
      union.update((Sketch) update);
    } else if (update instanceof String) {
      union.update((String) update);
    } else if (update instanceof byte[]) {
      union.update((byte[]) update);
    } else if (update instanceof Double) {
      union.update(((Double) update));
    } else if (update instanceof Integer || update instanceof Long) {
      union.update(((Number) update).longValue());
    } else if (update instanceof int[]) {
      union.update((int[]) update);
    } else if (update instanceof long[]) {
      union.update((long[]) update);
    } else if (update instanceof List) {
      for (Object entry : (List) update) {
        union.update(entry.toString());
      }
    } else {
      throw new ISE("Illegal type received while theta sketch merging [%s]", update.getClass());
    }
  }

  public static SketchAggregator create(final DimensionSelector selector, int size)
  {
    if (selector instanceof DimensionSelector.WithRawAccess) {
      final DimensionSelector.WithRawAccess rawAccess = (DimensionSelector.WithRawAccess)selector;
      return new SketchAggregator(size)
      {
        @Override
        public void aggregate()
        {
          final IndexedInts indexed = selector.getRow();
          final int length = indexed.size();
          if (length == 1) {
            union.update(rawAccess.lookupRaw(indexed.get(0)));
          } else if (length > 1) {
            for (int i = 0; i < length; i++) {
              union.update(rawAccess.lookupRaw(indexed.get(i)));
            }
          }
        }
      };
    } else {
      return new SketchAggregator(size)
      {
        @Override
        public void aggregate()
        {
          final IndexedInts indexed = selector.getRow();
          final int length = indexed.size();
          if (length == 1) {
            union.update(selector.lookupName(indexed.get(0)));
          } else if (length > 1) {
            for (int i = 0; i < length; i++) {
              union.update(selector.lookupName(indexed.get(i)));
            }
          }
        }
      };
    }
  }

  public static SketchAggregator create(final ObjectColumnSelector selector, int size)
  {
    return new SketchAggregator(size)
    {
      @Override
      public void aggregate()
      {
        final Object update = selector.get();
        if (update == null) {
          return;
        }

        updateUnion(union, update);
      }
    };
  }
}
