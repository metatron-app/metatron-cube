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

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;

import java.util.Comparator;

@JsonTypeName("quantile")
public class QuantilePostAggregator extends ApproximateHistogramPostAggregator
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Double.compare(((Number) o).doubleValue(), ((Number) o1).doubleValue());
    }
  };

  private final float probability;

  @JsonCreator
  public QuantilePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("probability") float probability
  )
  {
    super(name, fieldName);
    this.probability = probability;
    if (probability < 0 | probability > 1) {
      throw new IAE("Illegal probability[%s], must be strictly between 0 and 1", probability);
    }
  }

  @JsonProperty
  public float getProbability()
  {
    return probability;
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.FLOAT;
  }

  @Override
  protected Object computeFrom(ApproximateHistogramHolder holder)
  {
    return holder.getQuantiles(new float[]{probability})[0];
  }

  @Override
  public String toString()
  {
    return "QuantilePostAggregator{" +
           "probability=" + probability +
           ", name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
