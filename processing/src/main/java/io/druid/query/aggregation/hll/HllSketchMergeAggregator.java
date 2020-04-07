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
public class HllSketchMergeAggregator implements Aggregator<Union>
{
  private final ObjectColumnSelector<HllSketch> selector;
  private final TgtHllType tgtHllType;
  private final int lgK;

  public HllSketchMergeAggregator(
      final ObjectColumnSelector<HllSketch> selector,
      final int lgK,
      final TgtHllType tgtHllType
  )
  {
    this.selector = selector;
    this.tgtHllType = tgtHllType;
    this.lgK = lgK;
  }

  @Override
  public Union aggregate(Union current)
  {
    final HllSketch sketch = selector.get();
    if (sketch == null) {
      return current;
    }
    if (current == null) {
      current = new Union(lgK);
    }
    current.update(sketch);
    return current;
  }

  @Override
  public Object get(Union current)
  {
    return current == null ? null : current.getResult(tgtHllType);
  }
}
