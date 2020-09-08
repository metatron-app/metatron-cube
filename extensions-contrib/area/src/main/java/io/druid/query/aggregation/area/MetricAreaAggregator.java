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

package io.druid.query.aggregation.area;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

public class MetricAreaAggregator implements Aggregator.Simple<MetricArea>
{
  public static final Comparator<MetricArea> COMPARATOR = new Comparator<MetricArea>()
  {
    @Override
    public int compare(MetricArea ma1, MetricArea ma2)
    {
      return Double.compare(ma1.getArea(), ma2.getArea());
    }
  };

  public static MetricArea combine(Object lma, Object rma)
  {
    return ((MetricArea) lma).add(rma);
  }

  private final ObjectColumnSelector selector;

  public MetricAreaAggregator(ObjectColumnSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public MetricArea aggregate(MetricArea current)
  {
    final Object o = selector.get();
    if (o == null) {
      return current;
    }
    if (current == null) {
      current = new MetricArea();
    }
    current.add(o);
    return current;
  }
}
