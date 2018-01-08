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
import com.yahoo.sketches.quantiles.ItemsSketch;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeName("sketch.quantiles")
public class SketchQuantilesPostAggregator implements PostAggregator
{
  public static SketchQuantilesPostAggregator fraction(String name, String fieldName, double fraction)
  {
    return new SketchQuantilesPostAggregator(
        name, fieldName, SketchQuantilesOp.QUANTILES, fraction, null, null, null, null, null
    );
  }

  public static SketchQuantilesPostAggregator fractions(String name, String fieldName, double[] fractions)
  {
    return new SketchQuantilesPostAggregator(
        name, fieldName, SketchQuantilesOp.QUANTILES, null, fractions, null, null, null, null
    );
  }

  public static SketchQuantilesPostAggregator evenSpaced(String name, String fieldName, int evenSpaced)
  {
    return new SketchQuantilesPostAggregator(
        name, fieldName, SketchQuantilesOp.QUANTILES, null, null, evenSpaced, null, null, null
    );
  }

  private final String name;
  private final String fieldName;
  private final SketchQuantilesOp op;
  private final Object parameter;

  @JsonCreator
  public SketchQuantilesPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("op") SketchQuantilesOp op,
      @JsonProperty("fraction") Double fraction,
      @JsonProperty("fractions") double[] fractions,
      @JsonProperty("evenSpaced") Integer evenSpaced,
      @JsonProperty("evenCounted") Integer evenCounted,
      @JsonProperty("slopedSpaced") Integer slopedSpaced,
      @JsonProperty("splitPoints") String[] splitPoints
  )
  {
    this.name = Preconditions.checkNotNull(name, "'name' cannot be null");
    this.fieldName = Preconditions.checkNotNull(fieldName, "'fieldName' cannot be null");
    this.op = op == null ? SketchQuantilesOp.QUANTILES : op;
    if (op == null || op == SketchQuantilesOp.QUANTILES) {
      parameter = fraction != null ? fraction :
                  fractions != null ? fractions :
                  evenSpaced != null && evenSpaced > 0 ? SketchQuantilesOp.evenSpaced(evenSpaced) :
                  evenCounted != null && evenCounted > 0 ? SketchQuantilesOp.evenCounted(evenCounted) :
                  slopedSpaced != null && slopedSpaced > 0 ? SketchQuantilesOp.slopedSpaced(slopedSpaced) :
                  SketchQuantilesOp.DEFAULT_QUANTILE_PARAM;
    } else {
      parameter = splitPoints;
    }
    Preconditions.checkArgument(op == SketchQuantilesOp.IQR || parameter != null);
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
    return Ordering.natural();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
  {
    TypedSketch<ItemsSketch> sketch = (TypedSketch<ItemsSketch>) combinedAggregators.get(fieldName);
    return sketch == null ? null : op.calculate(sketch.value(), parameter);
  }
}
