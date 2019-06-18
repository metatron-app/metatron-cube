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

package io.druid.query.aggregation.distinctcount;

import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.WrappedRoaringBitmap;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DistinctCountBufferAggregator implements BufferAggregator
{
  private final DimensionSelector selector;
  private final ValueMatcher predicate;
  private final Map<Integer, MutableBitmap> mutableBitmapCollection = new HashMap<>();

  public DistinctCountBufferAggregator(
      DimensionSelector selector,
      ValueMatcher predicate
  )
  {
    this.selector = selector;
    this.predicate = predicate;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, 0L);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (predicate.matches()) {
      MutableBitmap mutableBitmap = getMutableBitmap(buf, position);
      for (final Integer index : selector.getRow()) {
        mutableBitmap.add(index);
      }
      buf.putLong(position, mutableBitmap.size());
    }
  }

  private MutableBitmap getMutableBitmap(ByteBuffer buf, int position)
  {
    MutableBitmap mutableBitmap = mutableBitmapCollection.get(position);
    if (mutableBitmap == null) {
      mutableBitmap = new WrappedRoaringBitmap();
      mutableBitmapCollection.put(position, mutableBitmap);
    }
    return mutableBitmap;
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public Float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getLong(position);
  }

  @Override
  public Double getDouble(ByteBuffer buf, int position)
  {
    return (double) buf.getLong(position);
  }

  @Override
  public Long getLong(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public void close()
  {
    mutableBitmapCollection.clear();
  }
}
