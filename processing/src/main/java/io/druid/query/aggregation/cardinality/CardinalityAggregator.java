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

package io.druid.query.aggregation.cardinality;

import io.druid.common.utils.Murmur3;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.Aggregator.StreamingSupport;
import io.druid.query.aggregation.HashAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;
import org.roaringbitmap.IntIterator;

import java.util.List;

public class CardinalityAggregator extends HashAggregator<HyperLogLogCollector>
    implements StreamingSupport<HyperLogLogCollector>, Aggregator.Scannable<HyperLogLogCollector>
{
  private final int b;

  public CardinalityAggregator(
      ValueMatcher predicate,
      List<DimensionSelector> selectorList,
      int[][] groupings,
      boolean byRow,
      int b
  )
  {
    super(predicate, selectorList, groupings, byRow);
    this.b = b;
  }

  public CardinalityAggregator(List<DimensionSelector> selectorList, boolean byRow)
  {
    this(ValueMatcher.TRUE, selectorList, null, byRow, 0);
  }

  @Override
  protected Class<HyperLogLogCollector> collectorClass()
  {
    return HyperLogLogCollector.class;
  }

  @Override
  protected HyperLogLogCollector newCollector()
  {
    return HyperLogLogCollector.makeLatestCollector(b);
  }

  @Override
  public Aggregator<HyperLogLogCollector> streaming()
  {
    return new CardinalityAggregator(predicate, selectorList, groupings, byRow, b)
    {
      private final HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector(b);

      @Override
      protected HyperLogLogCollector newCollector()
      {
        return collector.clear();
      }

      @Override
      public byte[] get(HyperLogLogCollector current)
      {
        return current == null ? null : current.toByteArray();
      }
    };
  }

  @Override
  public boolean supports()
  {
    return predicate == ValueMatcher.TRUE &&
           selectorList.size() == 1 &&
           selectorList.get(0) instanceof DimensionSelector.Scannable;
  }

  @Override
  public Object aggregate(IntIterator iterator)
  {
    DimensionSelector.Scannable selector = (DimensionSelector.Scannable) selectorList.get(0);
    HyperLogLogCollector collector = newCollector();
    selector.scan(iterator, (ix, buffer, offset, length) -> collector.add(Murmur3.hash64(buffer, offset, length)));
    return collector;
  }
}
