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

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.yahoo.sketches.hll.HllSketch;
import io.druid.data.ValueDesc;
import io.druid.segment.serde.ComplexMetrics;

/**
 * This module is to support count-distinct operations using {@link HllSketch}.
 * See <a href="https://datasketches.github.io/docs/HLL/HLL.html">HyperLogLog Sketch documentation</a>
 *
 * @author Alexander Saydakov
 */
public class HllSketchModule extends SimpleModule
{
  public static final ValueDesc TYPE = ValueDesc.of("HLLSketch"); // common type name to be associated with segment data
  public static final ValueDesc BUILD_TYPE = ValueDesc.of("HLLSketchBuild");
  public static final ValueDesc MERGE_TYPE = ValueDesc.of("HLLSketchMerge");
  public static final ValueDesc TO_STRING_TYPE = ValueDesc.of("HLLSketchToString");
  public static final ValueDesc UNION_TYPE = ValueDesc.of("HLLSketchUnion");
  public static final ValueDesc ESTIMATE_WITH_BOUNDS_TYPE = ValueDesc.of("HLLSketchEstimateWithBounds");

  public static final byte HLL_SKETCH_BUILD_CACHE_TYPE_ID = 0x2E;
  public static final byte HLL_SKETCH_MERGE_CACHE_TYPE_ID = 0x2F;
  public static final byte HLL_SKETCH_UNION_CACHE_TYPE_ID = 0x30;
  public static final byte HLL_SKETCH_TO_STRING_CACHE_TYPE_ID = 0x31;
  public static final byte HLL_SKETCH_TO_ESTIMATE_AND_BOUNDS_CACHE_TYPE_ID = 0x32;

  public HllSketchModule()
  {
    super("HllSketchModule");
    if (ComplexMetrics.getSerdeForType(TYPE) == null) {
      ComplexMetrics.registerSerde(TYPE, new HllSketchMergeComplexMetricSerde());
    }
    if (ComplexMetrics.getSerdeForType(BUILD_TYPE) == null) {
      ComplexMetrics.registerSerde(BUILD_TYPE, new HllSketchBuildComplexMetricSerde());
    }
    if (ComplexMetrics.getSerdeForType(MERGE_TYPE) == null) {
      ComplexMetrics.registerSerde(MERGE_TYPE, new HllSketchMergeComplexMetricSerde());
    }
    registerSubtypes(
        new NamedType(HllSketchMergeAggregatorFactory.class, MERGE_TYPE.typeName()),
        new NamedType(HllSketchBuildAggregatorFactory.class, BUILD_TYPE.typeName()),
        new NamedType(HllSketchToStringPostAggregator.class, TO_STRING_TYPE.typeName()),
        new NamedType(HllSketchUnionPostAggregator.class, UNION_TYPE.typeName()),
        new NamedType(HllSketchToEstimateWithBoundsPostAggregator.class, ESTIMATE_WITH_BOUNDS_TYPE.typeName())
    );
    addSerializer(HllSketch.class, new HllSketchJsonSerializer());
  }
}
