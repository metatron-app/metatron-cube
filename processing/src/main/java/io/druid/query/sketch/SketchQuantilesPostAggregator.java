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
import com.google.common.collect.Ordering;
import com.yahoo.sketches.quantiles.ItemsSketch;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeName("sketch.quantiles")
public class SketchQuantilesPostAggregator extends PostAggregator.Stateless
{
  public static SketchQuantilesPostAggregator fraction(String name, String fieldName, double fraction)
  {
    return new SketchQuantilesPostAggregator(
        name, fieldName, QuantileOperation.QUANTILES, fraction, null, null, null, null, null, null, false, -1, false
    );
  }

  public static SketchQuantilesPostAggregator fractions(String name, String fieldName, double[] fractions)
  {
    return new SketchQuantilesPostAggregator(
        name, fieldName, QuantileOperation.QUANTILES, null, fractions, null, null, null, null, null, false, -1, false
    );
  }

  public static SketchQuantilesPostAggregator evenSpaced(String name, String fieldName, int evenSpaced)
  {
    return new SketchQuantilesPostAggregator(
        name, fieldName, QuantileOperation.QUANTILES, null, null, null, evenSpaced, null, null, null, false, -1, false
    );
  }

  private final String name;
  private final String fieldName;
  private final QuantileOperation op;
  private final Object parameter;

  @JsonCreator
  public SketchQuantilesPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("op") QuantileOperation op,
      @JsonProperty("fraction") Double fraction,
      @JsonProperty("fractions") double[] fractions,
      @JsonProperty("count") Integer count,
      @JsonProperty("evenSpaced") Integer evenSpaced,
      @JsonProperty("evenCounted") Integer evenCounted,
      @JsonProperty("slopedSpaced") Integer slopedSpaced,
      @JsonProperty("splitPoints") String[] splitPoints,
      @JsonProperty("ratioAsCount") boolean ratioAsCount,
      @JsonProperty("maxThreshold") int maxThreshold,
      @JsonProperty("dedup") boolean dedup
  )
  {
    this.name = Preconditions.checkNotNull(name, "'name' cannot be null");
    this.fieldName = Preconditions.checkNotNull(fieldName, "'fieldName' cannot be null");
    this.op = op == null ? QuantileOperation.QUANTILES : op;
    if (op == null ||
        op == QuantileOperation.QUANTILES ||
        op == QuantileOperation.QUANTILES_CDF ||
        op == QuantileOperation.QUANTILES_PMF) {
      Object parameter = fraction != null ? fraction :
                         fractions != null ? fractions :
                         count != null ? count :
                         evenSpaced != null && evenSpaced > 0 ? QuantileOperation.evenSpaced(evenSpaced, dedup) :
                         evenCounted != null && evenCounted > 0 ? QuantileOperation.evenCounted(evenCounted, dedup) :
                         slopedSpaced != null && slopedSpaced > 0 ? QuantileOperation.slopedSpaced(slopedSpaced, maxThreshold, dedup) :
                         QuantileOperation.DEFAULT_QUANTILE_PARAM;
      if (op == null || op == QuantileOperation.QUANTILES) {
        this.parameter = parameter;
      } else {
        this.parameter = QuantileOperation.quantileRatioParam(parameter, ratioAsCount);
      }
    } else if (op == QuantileOperation.CDF || op == QuantileOperation.PMF) {
      parameter = QuantileOperation.ratioParam(splitPoints, ratioAsCount);
    } else {
      parameter = splitPoints;
    }
    Preconditions.checkArgument(op == QuantileOperation.IQR || parameter != null);
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
  public ValueDesc resolve(TypeResolver bindings)
  {
    switch (op) {
      case QUANTILES: return parameter instanceof Number ? ValueDesc.DOUBLE : ValueDesc.DOUBLE_ARRAY;
      case CDF: return ValueDesc.DOUBLE_ARRAY;
      case PMF: return ValueDesc.DOUBLE_ARRAY;
      case QUANTILES_CDF: return ValueDesc.UNKNOWN;
      case QUANTILES_PMF: return ValueDesc.UNKNOWN;
      case IQR: return ValueDesc.DOUBLE;
      default:
        throw new IllegalArgumentException("invalid op " + op);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Processor createStateless()
  {
    return new AbstractProcessor()
    {
      @Override
      public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
      {
        TypedSketch<ItemsSketch> sketch = (TypedSketch<ItemsSketch>) combinedAggregators.get(fieldName);
        return sketch == null ? null : op.calculate(sketch.value(), parameter);
      }
    };
  }
}
