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
 *
 */
public abstract class DecimalSumBufferAggregator extends DecimalBufferAggregator
{
  public static DecimalSumBufferAggregator create(
      final ObjectColumnSelector<BigDecimal> selector,
      final ValueMatcher predicate,
      final DecimalMetricSerde metric
  )
  {
    return new DecimalSumBufferAggregator(metric)
    {
      @Override
      public final void aggregate(ByteBuffer buf, int position0, int position1)
      {
        if (predicate.matches()) {
          final BigDecimal decimal = selector.get();
          if (decimal != null) {
            final BigDecimal current = read(buf, position1);
            if (current == null) {
              write(buf, position1, decimal);
            } else {
              write(buf, position1, current.add(decimal));
            }
          }
        }
      }
    };
  }

  DecimalSumBufferAggregator(DecimalMetricSerde metric)
  {
    super(metric);
  }
}
