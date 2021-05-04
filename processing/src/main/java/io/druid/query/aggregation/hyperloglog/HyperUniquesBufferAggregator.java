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

package io.druid.query.aggregation.hyperloglog;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public class HyperUniquesBufferAggregator implements BufferAggregator
{
  private final ValueMatcher predicate;
  private final ObjectColumnSelector selector;
  private final HyperLogLogCollector.Context context;

  public HyperUniquesBufferAggregator(
      ValueMatcher predicate,
      ObjectColumnSelector selector,
      int b
  )
  {
    this.predicate = predicate;
    this.selector = selector;
    this.context = HyperLogLogCollector.getContext(b);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.position(position);
    buf.put(context.EMPTY_BYTES);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (predicate.matches()) {
      final HyperLogLogCollector collector = (HyperLogLogCollector) selector.get();
      if (collector != null) {
        HyperLogLogCollector.from(context, buf, position).fold(collector);
      }
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return HyperLogLogCollector.copy(context, buf, position);
  }
}
