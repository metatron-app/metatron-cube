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

package io.druid.query.sketch;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;

public class SketchModule extends SimpleModule
{
  public SketchModule()
  {
    super("sketch");
    registerSubtypes(SketchQuery.class);
    registerSubtypes(GenericSketchAggregatorFactory.class);

    registerSubtypes(SketchQuantilesPostAggregator.class);
    registerSubtypes(SketchThetaPostAggregator.class);

    registerSubtypes(SketchThetaProcessor.class);
    registerSubtypes(SketchQuantilesProcessor.class);
    registerSubtypes(SketchFrequencyProcessor.class);
    registerSubtypes(SketchSamplingProcessor.class);
    registerSubtypes(SimilarityProcessingOperator.class);

    addSerializer(Sketch.class, new SketchJsonSerializer());
    addSerializer(Union.class, new UnionJsonSerializer());
    addSerializer(ItemsSketch.class, new ItemsSketchJsonSerializer());
    addSerializer(com.yahoo.sketches.frequencies.ItemsSketch.class, new FrequencySketchJsonSerializer());
    addSerializer(ReservoirItemsSketch.class, new SamplingSketchJsonSerializer());
    addSerializer(Memory.class, new MemoryJsonSerializer());
  }
}
