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
import io.druid.query.aggregation.Aggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;

public class DistinctCountAggregator implements Aggregator
{
  private final DimensionSelector selector;
  private final MutableBitmap mutableBitmap;
  private final ValueMatcher predicate;

  public DistinctCountAggregator(
      DimensionSelector selector,
      MutableBitmap mutableBitmap,
      ValueMatcher predicate
  )
  {
    this.selector = selector;
    this.mutableBitmap = mutableBitmap;
    this.predicate = predicate;
  }

  @Override
  public void aggregate()
  {
    if (predicate.matches()) {
      synchronized (this) {
        for (final Integer index : selector.getRow()) {
          mutableBitmap.add(index);
        }
      }
    }
  }

  @Override
  public void reset()
  {
    mutableBitmap.clear();
  }

  @Override
  public Object get()
  {
    return mutableBitmap.size();
  }

  @Override
  public Float getFloat()
  {
    return (float) mutableBitmap.size();
  }

  @Override
  public void close()
  {
    mutableBitmap.clear();
  }

  @Override
  public Long getLong()
  {
    return (long) mutableBitmap.size();
  }

  @Override
  public Double getDouble()
  {
    return (double) mutableBitmap.size();
  }
}
