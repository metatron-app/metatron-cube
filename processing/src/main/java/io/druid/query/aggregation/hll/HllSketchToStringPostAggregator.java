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

package io.druid.query.aggregation.hll;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.hll.HllSketch;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Returns a human-readable summary of a given {@link HllSketch}.
 * This is a string returned by toString() method of the sketch.
 * This can be useful for debugging.
 *
 * @author Alexander Saydakov
 */
public class HllSketchToStringPostAggregator extends PostAggregator.Abstract
{
  private final String name;
  private final PostAggregator field;

  @JsonCreator
  public HllSketchToStringPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field
  )
  {
    this.name = name;
    this.field = field;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return field.getDependentFields();
  }

  @Override
  public Comparator<String> getComparator()
  {
    return GuavaUtils.nullFirstNatural();
  }

  @Override
  public Processor processor()
  {
    return new AbstractProcessor()
    {
      private final Processor processor = field.processor();

      @Override
      public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
      {
        return processor.compute(timestamp, combinedAggregators).toString();
      }
    };
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.STRING;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "name='" + name + '\'' +
           ", field=" + field +
           "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HllSketchToStringPostAggregator)) {
      return false;
    }

    final HllSketchToStringPostAggregator that = (HllSketchToStringPostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }

    return field.equals(that.field);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field);
  }
}
