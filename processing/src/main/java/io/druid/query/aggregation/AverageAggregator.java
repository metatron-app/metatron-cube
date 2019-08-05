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
import io.druid.segment.DoubleColumnSelector;

/**
 */
public class AverageAggregator extends Aggregator.Abstract<AverageAggregator.Avr>
{
  private final ValueMatcher predicate;
  private final DoubleColumnSelector selector;

  public AverageAggregator(ValueMatcher predicate, DoubleColumnSelector selector)
  {
    this.predicate = predicate;
    this.selector = selector;
  }

  @Override
  public Avr aggregate(Avr current)
  {
    if (predicate.matches()) {
      final Double v = selector.get();
      if (v != null) {
        if (current == null) {
          current = new Avr();
        }
        current.count++;
        current.sum += v;
      }
    }
    return current;
  }

  @Override
  public Object get(Avr current)
  {
    return current == null ? null : new long[]{current.count, Double.doubleToLongBits(current.sum)};
  }

  public static final class Avr
  {
    private long count;
    private double sum;
  }
}
