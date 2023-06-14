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

package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.joda.time.Period;

/**
 */
public class SimpleBalancerStrategyFactory implements BalancerStrategyFactory
{
  private final Period offsetPeriod;
  private final Integer initialGrouping;
  private final Float baselineRatio;
  private final Float tolerance;

  public SimpleBalancerStrategyFactory(
      @JsonProperty("offsetPeriod") Period offsetPeriod,
      @JsonProperty("initialGrouping") Integer initialGrouping,
      @JsonProperty("baselineRatio") Float baselineRatio,
      @JsonProperty("tolerance") Float tolerance
  )
  {
    this.offsetPeriod = offsetPeriod;
    this.initialGrouping = initialGrouping;
    this.baselineRatio = baselineRatio;
    this.tolerance = tolerance;
  }

  @Override
  public BalancerStrategy createBalancerStrategy(ListeningExecutorService exec)
  {
    return new SimpleBalancerStrategy(offsetPeriod, initialGrouping, baselineRatio, tolerance);
  }
}
