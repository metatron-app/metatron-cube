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

package io.druid.query.aggregation.variance;

import io.druid.data.ValueDesc;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.stats.DruidStatsModule;

import java.util.Arrays;
import java.util.List;

/**
 */
public class VarianceTestHelper extends QueryRunnerTestHelper
{
  static {
    DruidStatsModule module = new DruidStatsModule();
    module.configure(null);
  }

  public static final String indexVarianceMetric = "index_var";
  public static final String indexDiv2VarianceMetric = "index_div2_var";

  public static final VarianceAggregatorFactory indexVarianceAggr = new VarianceAggregatorFactory(
      indexVarianceMetric,
      indexMetric,
      ValueDesc.DOUBLE
  );

  public static final VarianceAggregatorFactory indexDiv2VarianceAggr = new VarianceAggregatorFactory(
      indexDiv2VarianceMetric,
      null,
      "index / 2",
      null,
      null,
      ValueDesc.DOUBLE
  );

  public static final String stddevOfIndexMetric = "index_stddev";

  public static final PostAggregator stddevOfIndexPostAggr = new StandardDeviationPostAggregator(
      stddevOfIndexMetric,
      indexVarianceMetric,
      null
  );

  public static final List<AggregatorFactory> commonPlusVarAggregators = Arrays.<AggregatorFactory>asList(
      rowsCount,
      indexDoubleSum,
      qualityUniques,
      indexVarianceAggr
  );
}
