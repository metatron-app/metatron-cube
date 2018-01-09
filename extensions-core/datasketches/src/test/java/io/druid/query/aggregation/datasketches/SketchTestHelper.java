/*
 * Licensed to Metaqualitys Group Inc. (Metaqualitys) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metaqualitys licenses this file
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

package io.druid.query.aggregation.datasketches;

import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.stats.DruidStatsModule;
import io.druid.query.sketch.GenericSketchAggregatorFactory;
import io.druid.query.sketch.SketchOp;
import io.druid.query.sketch.SketchQuantilesPostAggregator;
import io.druid.query.sketch.SketchThetaPostAggregator;

/**
 */
public class SketchTestHelper extends QueryRunnerTestHelper
{
  static {
    DruidStatsModule module = new DruidStatsModule();
    module.configure(null);
  }

  public static final String indexSketchMetric = "index_sketch";

  public static final AggregatorFactory indexQuantileAggr = new GenericSketchAggregatorFactory(
      indexSketchMetric,
      indexMetric,
      SketchOp.QUANTILE,
      null,
      null,
      false
  );

  public static final AggregatorFactory indexThetaAggr = new GenericSketchAggregatorFactory(
      indexSketchMetric,
      indexMetric,
      SketchOp.THETA,
      null,
      null,
      false
  );

  public static final String qualitySketchMetric = "quality_sketch";

  public static final AggregatorFactory qualityQuantileAggr = new GenericSketchAggregatorFactory(
      qualitySketchMetric,
      qualityDimension,
      SketchOp.QUANTILE,
      null,
      null,
      false
  );

  public static final AggregatorFactory qualityThetaAggr = new GenericSketchAggregatorFactory(
      qualitySketchMetric,
      qualityDimension,
      SketchOp.THETA,
      null,
      null,
      false
  );

  public static final String quantilesOfIndexMetric = "index_quantiles";

  public static final PostAggregator quantilesOfIndexPostAggr = SketchQuantilesPostAggregator.fractions(
      quantilesOfIndexMetric,
      indexSketchMetric,
      new double[]{0.1, 0.5, 0.9}
  );

  public static final String quantilesOfQualityMetric = "quality_quantiles";

  public static final PostAggregator quantilesOfQualityPostAggr = SketchQuantilesPostAggregator.fractions(
      quantilesOfQualityMetric,
      qualitySketchMetric,
      new double[]{0.1, 0.5, 0.9}
  );

  public static final String cardinalityOfIndexMetric = "index_cardinality";

  public static final PostAggregator cardinalityOfIndexPostAggr = new SketchThetaPostAggregator(
      cardinalityOfIndexMetric,
      indexSketchMetric,
      true
  );

  public static final String cardinalityOfQualityMetric = "quality_cardinality";

  public static final PostAggregator cardinalityOfQualityPostAggr = new SketchThetaPostAggregator(
      cardinalityOfQualityMetric,
      qualitySketchMetric,
      true
  );
}
