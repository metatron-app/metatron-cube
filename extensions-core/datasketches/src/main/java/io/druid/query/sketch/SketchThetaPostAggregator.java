/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.yahoo.sketches.theta.Sketch;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeName("sketch.theta")
public class SketchThetaPostAggregator implements PostAggregator
{
  private final String name;
  private final String fieldName;
  private final boolean round;

  @JsonCreator
  public SketchThetaPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("round") boolean round
  )
  {
    this.name = Preconditions.checkNotNull(name, "'name' cannot be null");
    this.fieldName = Preconditions.checkNotNull(fieldName, "'fieldName' cannot be null");
    this.round = round;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return ImmutableSet.of(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    return Ordering.natural().nullsFirst();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
  {
    TypedSketch<Sketch> sketch = (TypedSketch<Sketch>) combinedAggregators.get(fieldName);
    if (sketch != null) {
      double estimate = sketch.value().getEstimate();
      return round ? Math.round(estimate) : estimate;
    }
    return null;
  }
}
