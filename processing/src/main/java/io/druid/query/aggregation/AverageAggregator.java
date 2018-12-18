/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;

/**
 */
public class AverageAggregator extends Aggregator.Abstract
{
  private long count;
  private double sum;
  private final ValueMatcher predicate;
  private final DoubleColumnSelector selector;

  public AverageAggregator(ValueMatcher predicate, DoubleColumnSelector selector)
  {
    this.predicate = predicate;
    this.selector = selector;
  }

  @Override
  public void aggregate()
  {
    if (predicate.matches()) {
      synchronized (this) {
        count++;
        sum += selector.get();
      }
    }
  }

  @Override
  public void reset()
  {
    count = 0;
    sum = 0;
  }

  @Override
  public Object get()
  {
    return new long[]{count, Double.doubleToLongBits(sum)};
  }
}
