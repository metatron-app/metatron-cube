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

package io.druid.query.aggregation;

import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class HashBufferAggregator<T extends HashCollector> extends HashIterator<T>
    implements BufferAggregator
{
  public HashBufferAggregator(
      ValueMatcher predicate,
      List<DimensionSelector> selectorList,
      int[][] groupings,
      boolean byRow
  )
  {
    super(predicate, selectorList, groupings, byRow);
  }

  public HashBufferAggregator(List<DimensionSelector> selectorList, int[][] groupings)
  {
    this(null, selectorList, groupings, true);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position0, int position1)
  {
    if (predicate.matches()) {
      consumer.accept(toCollector(buf, position1));
    }
  }

  protected abstract T toCollector(ByteBuffer buf, int position);
}
