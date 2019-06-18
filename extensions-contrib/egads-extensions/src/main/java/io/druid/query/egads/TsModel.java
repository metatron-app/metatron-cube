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

package io.druid.query.egads;

import com.yahoo.egads.models.tsmm.TimeSeriesModel;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 */
public enum TsModel
{
  DoubleExponentialSmoothingModel,
  MovingAverageModel,
  MultipleLinearRegressionModel,
  NaiveForecastingModel,
  OlympicModel {
    @Override
    public List<Parameter> parameters()
    {
      return Arrays.asList(
          Parameter.of("TIME_SHIFTS", int[].class),
          Parameter.of("BASE_WINDOWS", int[].class),
          Parameter.of("NUM_WEEKS", int.class),
          Parameter.of("NUM_TO_DROP", int.class),
          Parameter.optional("DYNAMIC_PARAMETERS", int.class, null)
      );
    }
  },
  PolynomialRegressionModel,
  RegressionModel,
  SimpleExponentialSmoothingModel,
  TripleExponentialSmoothingModel,
  WeightedMovingAverageModel,
  SpectralSmootherModel {
    @Override
    public List<Parameter> parameters()
    {
      return Arrays.asList(
          Parameter.of("WINDOW_SIZE", int.class),
          Parameter.of("FILTERING_METHOD", FilteringMethod.class),
          Parameter.optional("FILTERING_PARAM", double.class, null)
      );
    }
  },
  Null;

  public List<Parameter> parameters()
  {
    return Arrays.asList();
  }

  public TimeSeriesModel newInstance(Properties properties)
  {
    return Utils.newInstance("com.yahoo.egads.models.tsmm." + name(), TimeSeriesModel.class, properties);
  }
}
