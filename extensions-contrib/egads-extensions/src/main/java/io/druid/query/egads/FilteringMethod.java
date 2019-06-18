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

/**
 */
public enum FilteringMethod
{
  GAP_RATIO(0.01),
  EIGEN_RATIO(0.1),
  EXPLICIT(10),
  K_GAP(8),
  VARIANCE(0.99),
  SMOOTHNESS(0.97);

  private final double param;

  FilteringMethod(double param)
  {
    this.param = param;
  }

  public double param()
  {
    return param;
  }
}
