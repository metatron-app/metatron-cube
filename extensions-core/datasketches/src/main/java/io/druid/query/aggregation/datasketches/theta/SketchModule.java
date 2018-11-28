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

package io.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.initialization.DruidModule;
import io.druid.query.sketch.FrequencySketchJsonSerializer;
import io.druid.query.sketch.ItemsSketchJsonSerializer;
import io.druid.query.sketch.MemoryJsonSerializer;
import io.druid.query.sketch.SamplingSketchJsonSerializer;
import io.druid.query.sketch.SketchJsonSerializer;
import io.druid.query.sketch.SummaryPostProcessor;
import io.druid.query.sketch.SummaryQuery;
import io.druid.query.sketch.UnionJsonSerializer;
import io.druid.segment.serde.ComplexMetrics;

import java.util.Arrays;
import java.util.List;

public class SketchModule implements DruidModule
{
  public static final String THETA_SKETCH = "thetaSketch";

  public static final String THETA_SKETCH_MERGE_AGG = "thetaSketchMerge";
  public static final String THETA_SKETCH_BUILD_AGG = "thetaSketchBuild";

  public static final String THETA_SKETCH_ESTIMATE_POST_AGG = "thetaSketchEstimate";
  public static final String THETA_SKETCH_SET_OP_POST_AGG = "thetaSketchSetOp";

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(THETA_SKETCH) == null) {
      ComplexMetrics.registerSerde(THETA_SKETCH, new SketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(THETA_SKETCH_MERGE_AGG) == null) {
      ComplexMetrics.registerSerde(THETA_SKETCH_MERGE_AGG, new SketchMergeComplexMetricSerde());
    }

    if (ComplexMetrics.getSerdeForType(THETA_SKETCH_BUILD_AGG) == null) {
      ComplexMetrics.registerSerde(THETA_SKETCH_BUILD_AGG, new SketchBuildComplexMetricSerde());
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("ThetaSketchModule")
            .registerSubtypes(
                new NamedType(SketchMergeAggregatorFactory.class, THETA_SKETCH),
                new NamedType(SketchEstimatePostAggregator.class, THETA_SKETCH_ESTIMATE_POST_AGG),
                new NamedType(SketchSetPostAggregator.class, THETA_SKETCH_SET_OP_POST_AGG)
            )
            .registerSubtypes(SummaryQuery.class)
            .registerSubtypes(SummaryPostProcessor.class)
            .registerSubtypes(SketchEstimatePostProcessor.class)
    );
  }
}
