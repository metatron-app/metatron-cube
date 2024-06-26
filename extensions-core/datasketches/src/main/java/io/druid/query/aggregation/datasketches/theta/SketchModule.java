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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.druid.data.ValueDesc;
import io.druid.initialization.DruidModule;
import io.druid.query.sketch.SummaryPostProcessor;
import io.druid.query.sketch.SummaryQuery;
import io.druid.segment.serde.ComplexMetrics;

import java.util.Arrays;
import java.util.List;

public class SketchModule implements DruidModule
{
  public static final ValueDesc THETA_SKETCH = ValueDesc.of("thetaSketch");

  public static final ValueDesc THETA_SKETCH_MERGE_AGG = ValueDesc.of("thetaSketchMerge");
  public static final ValueDesc THETA_SKETCH_BUILD_AGG = ValueDesc.of("thetaSketchBuild");

  public static final ValueDesc THETA_SKETCH_ESTIMATE_POST_AGG = ValueDesc.of("thetaSketchEstimate");
  public static final ValueDesc THETA_SKETCH_SET_OP_POST_AGG = ValueDesc.of("thetaSketchSetOp");

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
                new NamedType(SketchMergeAggregatorFactory.class, THETA_SKETCH.typeName()),
                new NamedType(SketchEstimatePostAggregator.class, THETA_SKETCH_ESTIMATE_POST_AGG.typeName()),
                new NamedType(SketchSetPostAggregator.class, THETA_SKETCH_SET_OP_POST_AGG.typeName())
            )
            .registerSubtypes(SummaryQuery.class)
            .registerSubtypes(SummaryPostProcessor.class)
            .registerSubtypes(SketchEstimatePostProcessor.class)
    );
  }
}
