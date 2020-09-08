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

package io.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.yahoo.sketches.theta.Sketch;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SketchEstimatePostAggregator extends PostAggregator.Abstract
{
  private final String name;
  private final PostAggregator field;
  private final Integer errorBoundsStdDev;

  @JsonCreator
  public SketchEstimatePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("field") PostAggregator field,
      @JsonProperty("errorBoundsStdDev") Integer errorBoundsStdDev
  )
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.errorBoundsStdDev = errorBoundsStdDev;
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = Sets.newHashSet();
    dependentFields.addAll(field.getDependentFields());
    return dependentFields;
  }

  @Override
  public Comparator getComparator()
  {
    if (errorBoundsStdDev == null) {
      return GuavaUtils.NULL_FIRST_NATURAL;
    } else {
      return new Comparator()
      {
        @Override
        public int compare(Object o1, Object o2)
        {
          return Double.compare(
              ((SketchEstimateWithErrorBounds) o1).getEstimate(),
              ((SketchEstimateWithErrorBounds) o2).getEstimate()
          );
        }
      };
    }
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
        Sketch sketch = SketchSetPostAggregator.toSketch(processor.compute(timestamp, combinedAggregators));
        if (errorBoundsStdDev != null) {
          return new SketchEstimateWithErrorBounds(
              sketch.getEstimate(),
              sketch.getUpperBound(errorBoundsStdDev),
              sketch.getLowerBound(errorBoundsStdDev),
              errorBoundsStdDev
          );
        } else {
          return sketch.getEstimate();
        }
      }
    };
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return errorBoundsStdDev == null ? ValueDesc.DOUBLE_ARRAY : ValueDesc.UNKNOWN;
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

  @JsonProperty
  public Integer getErrorBoundsStdDev()
  {
    return errorBoundsStdDev;
  }

  @Override
  public String toString()
  {
    return "SketchEstimatePostAggregator{" +
           "name='" + name + '\'' +
           ", field=" + field +
           ", errorBoundsStdDev=" + errorBoundsStdDev +
           "}";
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

    SketchEstimatePostAggregator that = (SketchEstimatePostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }
    if (!Objects.equals(errorBoundsStdDev, that.errorBoundsStdDev)) {
      return false;
    }
    return field.equals(that.field);

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + field.hashCode();
    result = 31 * result + Objects.hashCode(errorBoundsStdDev);
    return result;
  }
}
