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
import com.google.inject.multibindings.MapBinder;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.theta.Sketch;
import io.druid.guice.LazySingleton;
import io.druid.guice.QueryToolBinders;
import io.druid.initialization.DruidModule;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.sketch.SimilarityProcessingOperator;
import io.druid.query.sketch.SketchQuery;
import io.druid.query.sketch.SketchQueryQueryToolChest;
import io.druid.query.sketch.SketchQueryRunnerFactory;
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

    if (binder != null) {
      // binder == null for tests
      MapBinder<Class<? extends Query>, QueryToolChest> toolChests = QueryToolBinders.queryToolChestBinder(binder);
      toolChests.addBinding(SketchQuery.class).to(SketchQueryQueryToolChest.class);
      binder.bind(SketchQueryQueryToolChest.class).in(LazySingleton.class);

      MapBinder<Class<? extends Query>, QueryRunnerFactory> factories = QueryToolBinders.queryRunnerFactoryBinder(binder);
      factories.addBinding(SketchQuery.class).to(SketchQueryRunnerFactory.class);
      binder.bind(SketchQueryRunnerFactory.class).in(LazySingleton.class);
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
            .registerSubtypes(SketchQuery.class)
            .registerSubtypes(SimilarityProcessingOperator.class)
            .addSerializer(
                Sketch.class, new SketchJsonSerializer()
            )
            .addSerializer(
                Memory.class, new MemoryJsonSerializer())
    );
  }
}
