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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class FieldAccessPostAggregator extends PostAggregator.Stateless
{
  public static FieldAccessPostAggregator of(String fieldName)
  {
    return new FieldAccessPostAggregator(null, fieldName);
  }

  public static FieldAccessPostAggregator of(String name, AggregatorFactory field)
  {
    return new FieldAccessPostAggregator(name, field.getName())
    {
      @Override
      public Comparator getComparator() {return field.getComparator();}
    };
  }

  private final String name;
  private final String fieldName;

  @JsonCreator
  public FieldAccessPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.name = name;
    this.fieldName = Preconditions.checkNotNull(fieldName, "'fieldName' should not be null");
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return bindings.resolve(fieldName);
  }

  @Override
  protected Processor createStateless()
  {
    return new AbstractProcessor()
    {
      @Override
      public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
      {
        return combinedAggregators.get(fieldName);
      }
    };
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public String toString()
  {
    return "FieldAccessPostAggregator{" +
           (name == null ? "": "name='" + name + "', ") +
           "fieldName='" + fieldName + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (!(o instanceof FieldAccessPostAggregator)) return false;

    FieldAccessPostAggregator that = (FieldAccessPostAggregator) o;

    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName);
  }
}
