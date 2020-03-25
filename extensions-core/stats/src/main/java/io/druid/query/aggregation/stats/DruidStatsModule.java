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

package io.druid.query.aggregation.stats;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import io.druid.initialization.DruidModule;
import io.druid.query.aggregation.AggregatorFactory.SQLBundle;
import io.druid.query.aggregation.corr.PearsonAggregatorFactory;
import io.druid.query.aggregation.corr.PearsonFoldingAggregatorFactory;
import io.druid.query.aggregation.corr.PearsonSerde;
import io.druid.query.aggregation.covariance.CovarianceAggregatorFactory;
import io.druid.query.aggregation.covariance.CovarianceFoldingAggregatorFactory;
import io.druid.query.aggregation.covariance.CovariancePostProcessor;
import io.druid.query.aggregation.covariance.CovarianceQuery;
import io.druid.query.aggregation.covariance.CovarianceSerde;
import io.druid.query.aggregation.kurtosis.KurtosisAggregatorFactory;
import io.druid.query.aggregation.kurtosis.KurtosisFoldingAggregatorFactory;
import io.druid.query.aggregation.kurtosis.KurtosisSerde;
import io.druid.query.aggregation.variance.StandardDeviationPostAggregator;
import io.druid.query.aggregation.variance.VarianceAggregatorFactory;
import io.druid.query.aggregation.variance.VarianceCombinedSerde;
import io.druid.query.aggregation.variance.VarianceFoldingAggregatorFactory;
import io.druid.query.aggregation.variance.VarianceSerde;
import io.druid.segment.serde.ComplexMetrics;

import java.util.List;

/**
 */
public class DruidStatsModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("stats").registerSubtypes(
            VarianceAggregatorFactory.class,
            VarianceFoldingAggregatorFactory.class,
            StandardDeviationPostAggregator.class,
            PearsonAggregatorFactory.class,
            PearsonFoldingAggregatorFactory.class,
            CovarianceAggregatorFactory.class,
            CovarianceFoldingAggregatorFactory.class,
            KurtosisAggregatorFactory.class,
            KurtosisFoldingAggregatorFactory.class,
            CovarianceQuery.class,
            CovariancePostProcessor.class
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType("variance") == null) {
      ComplexMetrics.registerSerde("variance", new VarianceSerde());
    }
    if (ComplexMetrics.getSerdeForType("varianceCombined") == null) {
      ComplexMetrics.registerSerde("varianceCombined", new VarianceCombinedSerde());
    }
    if (ComplexMetrics.getSerdeForType("pearson") == null) {
      ComplexMetrics.registerSerde("pearson", new PearsonSerde());
    }
    if (ComplexMetrics.getSerdeForType("covariance") == null) {
      ComplexMetrics.registerSerde("covariance", new CovarianceSerde());
    }
    if (ComplexMetrics.getSerdeForType("kurtosis") == null) {
      ComplexMetrics.registerSerde("kurtosis", new KurtosisSerde());
    }
    if (binder == null) {
      return; // in test
    }

    final Multibinder<SQLBundle> typedSet = Multibinder.newSetBinder(binder, SQLBundle.class);
    typedSet.addBinding().toInstance(
        new SQLBundle("variance", new VarianceAggregatorFactory("<name>", "<fieldName>"))
    );
    typedSet.addBinding().toInstance(
        new SQLBundle("var_pop", new VarianceAggregatorFactory("<name>", "<fieldName>", "population", null))
    );
    typedSet.addBinding().toInstance(
        new SQLBundle(
            "stddev",
            new VarianceAggregatorFactory("<name>", "<fieldName>"),
            new StandardDeviationPostAggregator("<name>", "<fieldName>", null)
        )
    );
    typedSet.addBinding().toInstance(
        new SQLBundle(
            "stddev_pop",
            new VarianceAggregatorFactory("<name>", "<fieldName>"),
            new StandardDeviationPostAggregator("<name>", "<fieldName>", "population")
        )
    );

    typedSet.addBinding().toInstance(
        new SQLBundle("pearson", new PearsonAggregatorFactory("<name>", "<fieldName1>", "<fieldName2>", null, null))
    );
    typedSet.addBinding().toInstance(
        new SQLBundle("covariance", new CovarianceAggregatorFactory("<name>", "<fieldName1>", "<fieldName2>", null, null))
    );
    typedSet.addBinding().toInstance(
        new SQLBundle("kurtosis", new KurtosisAggregatorFactory("<name>", "<fieldName1>", null, null))
    );
  }
}
