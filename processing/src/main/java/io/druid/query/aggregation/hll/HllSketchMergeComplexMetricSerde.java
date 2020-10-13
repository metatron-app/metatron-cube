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

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

public class HllSketchMergeComplexMetricSerde extends ComplexMetricSerde
{
  @Override
  public String getTypeName()
  {
    return HllSketchModule.TYPE_NAME; // must be common type name
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return HllSketchObjectStrategy.STRATEGY;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return HllSketch.class;
      }

      @Override
      public HllSketch extractValue(final Row inputRow, final String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        return object == null ? null : deserializeSketch(object);
      }
    };
  }

  static HllSketch deserializeSketch(final Object object)
  {
    if (object instanceof String) {
      return HllSketch.wrap(Memory.wrap(StringUtils.decodeBase64((String) object)));
    } else if (object instanceof byte[]) {
      return HllSketch.wrap(Memory.wrap((byte[]) object));
    } else if (object instanceof HllSketch) {
      return (HllSketch) object;
    }
    throw new IAE("Object is not of a type that can be deserialized to an HllSketch:" + object.getClass().getName());
  }
}
