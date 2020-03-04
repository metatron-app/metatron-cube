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

package io.druid.query.aggregation.hyperloglog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class HyperUniqueFinalizingPostAggregator extends PostAggregator.Stateless
{
  private static final Comparator DOUBLE_COMPARATOR = Ordering.from(new Comparator<Number>()
  {
    @Override
    public int compare(Number lhs, Number rhs)
    {
      return Double.compare(lhs.doubleValue(), rhs.doubleValue());
    }
  }).nullsFirst();

  private static final Comparator LONG_COMPARATOR = Ordering.from(new Comparator<Number>()
  {
    @Override
    public int compare(Number lhs, Number rhs)
    {
      return Long.compare(lhs.longValue(), rhs.longValue());
    }
  }).nullsFirst();

  private final String name;
  private final String fieldName;
  private final boolean round;

  @JsonCreator
  public HyperUniqueFinalizingPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("round") boolean round
  )
  {
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName is null");
    //Note that, in general, name shouldn't be null, we are defaulting
    //to fieldName here just to be backward compatible with 0.7.x
    this.name = name == null ? fieldName : name;
    this.round = round;
  }

  public HyperUniqueFinalizingPostAggregator(String name, String fieldName)
  {
    this(name, fieldName, false);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    return round ? LONG_COMPARATOR : DOUBLE_COMPARATOR;
  }

  @Override
  protected Processor createStateless()
  {
    return new AbstractProcessor()
    {
      @Override
      public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
      {
        return HyperUniquesAggregatorFactory.estimateCardinality(combinedAggregators.get(fieldName), round);
      }
    };
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return round ? ValueDesc.LONG : ValueDesc.DOUBLE;
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
  public boolean isRound()
  {
    return round;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HyperUniqueFinalizingPostAggregator that = (HyperUniqueFinalizingPostAggregator) o;
    return Objects.equals(name, that.name) &&
           Objects.equals(fieldName, that.fieldName) &&
           round == that.round;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName);
  }

  @Override
  public String toString()
  {
    return "HyperUniqueFinalizingPostAggregator{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", round='" + round + '\'' +
           '}';
  }
}
