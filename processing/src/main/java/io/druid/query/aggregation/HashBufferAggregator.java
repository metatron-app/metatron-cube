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
import java.util.function.Consumer;

public abstract class HashBufferAggregator<T extends HashCollector> extends HashIterator<T> implements BufferAggregator
{
  public HashBufferAggregator(
      ValueMatcher predicate,
      List<DimensionSelector> selectorList,
      int[][] groupings,
      boolean byRow,
      boolean needValue
  )
  {
    super(predicate, selectorList, groupings, byRow, needValue);
  }

  public HashBufferAggregator(List<DimensionSelector> selectorList, int[][] groupings, boolean needValue)
  {
    this(null, selectorList, groupings, true, needValue);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (predicate.matches()) {
      consumer.accept(toCollector(buf, position));
    }
  }

  protected abstract T toCollector(ByteBuffer buf, int position);

  public static abstract class ScanSupport<T extends HashCollector.ScanSupport> extends HashBufferAggregator<T>
  {
    public ScanSupport(
        ValueMatcher predicate,
        List<DimensionSelector> selectorList,
        int[][] groupings,
        boolean byRow
    )
    {
      super(predicate, selectorList, groupings, byRow, false);
    }

    @Override
    protected Consumer<T> toConsumer(List<DimensionSelector> selectorList)
    {
      if (selectorList.size() == 1 && selectorList.get(0) instanceof DimensionSelector.Scannable) {
        final DimensionSelector.Scannable selector = (DimensionSelector.Scannable) selectorList.get(0);
        return collector -> collector.collect(selector);
      }
      return super.toConsumer(selectorList);
    }
  }
}
