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

import io.druid.query.aggregation.Aggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ObjectColumnSelector;

/**
 */
public class HyperUniquesAggregator extends Aggregator.Abstract<HyperLogLogCollector>
{
  private final ValueMatcher predicate;
  private final ObjectColumnSelector selector;

  public HyperUniquesAggregator(ValueMatcher predicate, ObjectColumnSelector selector)
  {
    this.predicate = predicate;
    this.selector = selector;
  }

  @Override
  public HyperLogLogCollector aggregate(HyperLogLogCollector current)
  {
    if (predicate.matches()) {
      if (current == null) {
        current = HyperLogLogCollector.makeLatestCollector();
      }
      return current.fold((HyperLogLogCollector) selector.get());
    }
    return current;
  }
}
