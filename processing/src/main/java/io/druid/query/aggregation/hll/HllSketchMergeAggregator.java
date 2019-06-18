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

package io.druid.query.aggregation.hll;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

/**
 * This aggregator merges existing sketches.
 * The input column must contain {@link HllSketch}
 *
 * @author Alexander Saydakov
 */
public class HllSketchMergeAggregator extends Aggregator.Abstract
{
  private final ObjectColumnSelector<HllSketch> selector;
  private final TgtHllType tgtHllType;
  private final Union union;

  public HllSketchMergeAggregator(
      final ObjectColumnSelector<HllSketch> selector,
      final int lgK,
      final TgtHllType tgtHllType
  )
  {
    this.selector = selector;
    this.tgtHllType = tgtHllType;
    this.union = new Union(lgK);
  }

  /*
   * This method is synchronized because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently.
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public void aggregate()
  {
    final HllSketch sketch = selector.get();
    if (sketch == null) {
      return;
    }
    synchronized (this) {
      union.update(sketch);
    }
  }

  /*
   * This method is synchronized because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently.
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public synchronized Object get()
  {
    return union.getResult(tgtHllType);
  }
}
