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

package io.druid.query.aggregation.datasketches.theta;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.theta.Sketch;
import io.druid.data.ValueDesc;
import io.druid.query.sketch.ThetaOperations;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.MetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.util.List;

public class SketchMergeComplexMetricSerde implements ComplexMetricSerde
{
  private final SketchObjectStrategy strategy = new SketchObjectStrategy();

  @Override
  public ValueDesc getType()
  {
    return SketchModule.THETA_SKETCH;
  }

  @Override
  public MetricExtractor getExtractor(List<String> typeHint)
  {
    return new MetricExtractor()
    {
      @Override
      public Object extract(Object object)
      {
        if (object == null || object instanceof Sketch || object instanceof Memory) {
          return object;
        }
        return ThetaOperations.deserialize(object);
      }
    };
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return strategy;
  }
}
