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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.yahoo.sketches.theta.Sketch;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeName("sketch.theta")
public class SketchThetaPostAggregator extends PostAggregator.Stateless
{
  private final String name;
  private final String fieldName;
  private final int numStdDev;
  private final boolean round;

  @JsonCreator
  public SketchThetaPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("numStdDev") Integer numStdDev,
      @JsonProperty("round") boolean round
  )
  {
    this.name = Preconditions.checkNotNull(name, "'name' cannot be null");
    this.fieldName = Preconditions.checkNotNull(fieldName, "'fieldName' cannot be null");
    this.numStdDev = numStdDev == null ? -1 : numStdDev;
    this.round = round;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getNumStdDev()
  {
    return numStdDev;
  }

  @JsonProperty
  public boolean isRound()
  {
    return round;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return ImmutableSet.of(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    return GuavaUtils.nullFirstNatural();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Processor createStateless()
  {
    if (numStdDev > 0) {
      return new AbstractProcessor()
      {
        @Override
        public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
        {
          TypedSketch<Sketch> typed = (TypedSketch<Sketch>) combinedAggregators.get(fieldName);
          return typed == null ? null : toMap(typed.value(), numStdDev, round);
        }
      };
    }
    if (round) {
      return new AbstractProcessor()
      {
        @Override
        public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
        {
          TypedSketch<Sketch> typed = (TypedSketch<Sketch>) combinedAggregators.get(fieldName);
          return typed == null ? null : Math.round(typed.value().getEstimate());
        }
      };
    }
    return new AbstractProcessor()
    {
      @Override
      public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
      {
        TypedSketch<Sketch> typed = (TypedSketch<Sketch>) combinedAggregators.get(fieldName);
        return typed == null ? null : typed.value().getEstimate();
      }
    };
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return numStdDev > 0 ? ValueDesc.MAP : round ? ValueDesc.LONG : ValueDesc.DOUBLE;
  }

  static Map<String, Object> toMap(Sketch sketch, int numStdDev, boolean round)
  {
    Map<String, Object> result = Maps.newLinkedHashMap();
    result.put("estimate", round ? Math.round(sketch.getEstimate()) : sketch.getEstimate());
    result.put("upper95", round ? Math.round(sketch.getUpperBound(numStdDev)) : sketch.getUpperBound(numStdDev));
    result.put("lower95", round ? Math.round(sketch.getLowerBound(numStdDev)) : sketch.getLowerBound(numStdDev));
    return result;
  }
}
