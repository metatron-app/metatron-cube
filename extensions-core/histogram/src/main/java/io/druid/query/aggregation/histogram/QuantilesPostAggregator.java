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

import java.util.Arrays;
import java.util.Comparator;

@JsonTypeName("quantiles")
public class QuantilesPostAggregator extends ApproximateHistogramPostAggregator
{
  private final float[] probabilities;

  @JsonCreator
  public QuantilesPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("probabilities") float[] probabilities
  )
  {
    super(name, fieldName);
    this.probabilities = probabilities;
    for (float p : probabilities) {
      if (p < 0 | p > 1) {
        throw new IAE("Illegal probability[%s], must be strictly between 0 and 1", p);
      }
    }
  }

  @JsonProperty
  public float[] getProbabilities()
  {
    return probabilities;
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.FLOAT_ARRAY;
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Object computeFrom(ApproximateHistogramHolder holder)
  {
    return new Quantiles(probabilities, holder.getQuantiles(probabilities), holder.getMin(), holder.getMax());
  }

  @Override
  public String toString()
  {
    return "QuantilesPostAggregator{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", probabilities=" + Arrays.toString(probabilities) +
           '}';
  }
}
