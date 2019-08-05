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

import com.metamx.common.IAE;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

/**
 * This aggregator builds sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 *
 * @author Alexander Saydakov
 */
public class HllSketchBuildAggregator extends Aggregator.Abstract<HllSketch>
{
  private final ObjectColumnSelector<Object> selector;
  private final int lgK;
  private final TgtHllType tgtHllType;

  public HllSketchBuildAggregator(
      final ObjectColumnSelector<Object> selector,
      final int lgK,
      final TgtHllType tgtHllType
  )
  {
    this.selector = selector;
    this.lgK = lgK;
    this.tgtHllType = tgtHllType;
  }

  @Override
  public HllSketch aggregate(HllSketch current)
  {
    final Object value = selector.get();
    if (value == null) {
      return current;
    }
    if (current == null) {
      current = new HllSketch(lgK, tgtHllType);
    }
    updateSketch(current, value);
    return current;
  }

  static void updateSketch(final HllSketch sketch, final Object value)
  {
    if (value instanceof Integer || value instanceof Long) {
      sketch.update(((Number) value).longValue());
    } else if (value instanceof Float || value instanceof Double) {
      sketch.update(((Number) value).doubleValue());
    } else if (value instanceof String) {
      sketch.update(((String) value).toCharArray());
    } else if (value instanceof char[]) {
      sketch.update((char[]) value);
    } else if (value instanceof byte[]) {
      sketch.update((byte[]) value);
    } else if (value instanceof int[]) {
      sketch.update((int[]) value);
    } else if (value instanceof long[]) {
      sketch.update((long[]) value);
    } else {
      throw new IAE("Unsupported type " + value.getClass());
    }
  }
}
