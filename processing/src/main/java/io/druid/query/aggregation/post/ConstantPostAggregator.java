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

package io.druid.query.aggregation.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.RowResolver;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
public class ConstantPostAggregator extends PostAggregator.Stateless
{
  private final String name;
  private final Number constantValue;

  @JsonCreator
  public ConstantPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("value") Number constantValue
  )
  {
    this.name = name;
    this.constantValue = constantValue;
    Preconditions.checkNotNull(this.constantValue);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet();
  }

  @Override
  public Comparator getComparator()
  {
    return GuavaUtils.allEquals();
  }

  @Override
  protected Processor createStateless()
  {
    return new AbstractProcessor()
    {
      @Override
      public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
      {
        return constantValue;
      }
    };
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return RowResolver.toValueType(constantValue);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty("value")
  public Number getConstantValue()
  {
    return constantValue;
  }

  @Override
  public String toString()
  {
    return "ConstantPostAggregator{" +
           "name='" + name + '\'' +
           ", constantValue=" + constantValue +
           '}';
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

    ConstantPostAggregator that = (ConstantPostAggregator) o;

    if (constantValue != null && that.constantValue != null) {
      if (constantValue.doubleValue() != that.constantValue.doubleValue()) {
        return false;
      }
    } else if (!constantValue.equals(that.constantValue)) {
      return false;
    }

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (constantValue != null ? constantValue.hashCode() : 0);
    return result;
  }
}
