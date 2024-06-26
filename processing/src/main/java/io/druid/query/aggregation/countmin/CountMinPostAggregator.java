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

package io.druid.query.aggregation.countmin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class CountMinPostAggregator extends PostAggregator.Stateless
{
  private final String name;
  private final String fieldName;
  private final String value;

  @JsonCreator
  public CountMinPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("value") String value
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.value = value;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return ImmutableSet.of(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    return GuavaUtils.NULL_FIRST_NATURAL;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.LONG;
  }

  @Override
  protected Processor createStateless()
  {
    return new AbstractProcessor()
    {
      @Override
      public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
      {
        CountMinSketch sketch = (CountMinSketch) combinedAggregators.get(fieldName);
        return sketch == null ? null : sketch.getEstimatedCount(value.getBytes());
      }
    };
  }
}
