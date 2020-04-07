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
import io.druid.segment.data.IndexedInts;

public class DistinctCountAggregator implements Aggregator<MutableBitmap>
{
  private final DimensionSelector selector;
  private final BitMapFactory bitMapFactory;
  private final ValueMatcher predicate;

  public DistinctCountAggregator(
      DimensionSelector selector,
      BitMapFactory bitMapFactory,
      ValueMatcher predicate
  )
  {
    this.selector = selector;
    this.bitMapFactory = bitMapFactory;
    this.predicate = predicate;
  }

  @Override
  public MutableBitmap aggregate(MutableBitmap current)
  {
    if (predicate.matches()) {
      if (current == null) {
        current = bitMapFactory.makeEmptyMutableBitmap();
      }
      final IndexedInts row = selector.getRow();
      final int length = row.size();
      for (int i = 0; i < length; i++) {
        current.add(row.get(i));
      }
    }
    return current;
  }

  @Override
  public Object get(MutableBitmap current)
  {
    return current == null ? 0 : current.size();
  }
}
