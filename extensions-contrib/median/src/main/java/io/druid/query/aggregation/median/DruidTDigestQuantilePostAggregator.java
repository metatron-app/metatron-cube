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

package io.druid.query.aggregation.median;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("digestQuantile")
public class DruidTDigestQuantilePostAggregator extends DruidTDigestMedianPostAggregator
{
  private final double probability;

  @JsonCreator
  public DruidTDigestQuantilePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("probability") double probability
  )
  {
    super(name, fieldName);
    this.probability = probability;
  }

  @JsonProperty
  public double getProbability()
  {
    return probability;
  }

  @Override
  protected Object computeFrom(DruidTDigest digest)
  {
    return digest.quantile(probability);
  }

  @Override
  public String toString()
  {
    return "DruidTDigestQuantilePostAggregator{" +
        "name='" + name + '\'' +
        ", fieldName='" + fieldName + '\'' +
        ", probability=" + probability +
        '}';
  }
}