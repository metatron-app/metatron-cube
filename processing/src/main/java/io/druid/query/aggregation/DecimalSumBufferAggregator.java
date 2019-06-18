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
import io.druid.segment.ObjectColumnSelector;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 */
public abstract class DecimalSumBufferAggregator extends DecimalBufferAggregator
{
  public static DecimalSumBufferAggregator create(
      final ObjectColumnSelector<BigDecimal> selector,
      final ValueMatcher predicate,
      final DecimalMetricSerde metric
  )
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new DecimalSumBufferAggregator(metric)
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          Object decimal = selector.get();
          BigDecimal augend = (BigDecimal) decimal;
          if (augend != null) {
            write(buf, position, read(buf, position).add(augend));
          }
        }
      };
    } else {
      return new DecimalSumBufferAggregator(metric)
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          BigDecimal augend = selector.get();
          if (augend != null && predicate.matches()) {
            write(buf, position, read(buf, position).add(augend));
          }
        }
      };
    }
  }

  DecimalSumBufferAggregator(DecimalMetricSerde metric)
  {
    super(metric);
  }
}
