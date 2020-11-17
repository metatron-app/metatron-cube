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

package io.druid.query.aggregation.countmin;

import io.druid.query.aggregation.HashAggregator;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;

import java.util.List;

public class CountMinAggregator extends HashAggregator<CountMinSketch>
{
  private final int width;
  private final int depth;

  public CountMinAggregator(
      ValueMatcher predicate,
      List<DimensionSelector> selectorList,
      int[][] groupings,
      boolean byRow,
      int width,
      int depth
  )
  {
    super(predicate, selectorList, groupings, byRow, false);
    this.width = width;
    this.depth = depth;
  }

  @Override
  protected final CountMinSketch newCollector()
  {
    return new CountMinSketch(width, depth);
  }
}
