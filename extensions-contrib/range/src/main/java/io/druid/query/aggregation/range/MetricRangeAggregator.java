/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.query.aggregation.range;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

public class MetricRangeAggregator implements Aggregator.Simple<MetricRange>
{
  public static final Comparator<MetricRange> COMPARATOR = new Comparator<MetricRange>()
  {
    @Override
    public int compare(MetricRange o1, MetricRange o2)
    {
      return Double.compare(o1.getRange(), o2.getRange());
    }
  };

  public static MetricRange combine(Object lmr, Object rmr)
  {
    return ((MetricRange)lmr).add(rmr);
  }

  private final ObjectColumnSelector selector;

  public MetricRangeAggregator(ObjectColumnSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public MetricRange aggregate(MetricRange current)
  {
    final Object o = selector.get();
    if (o == null) {
      return current;
    }
    if (current == null) {
      current = new MetricRange();
    }
    current.add(o);
    return current;
  }
}
