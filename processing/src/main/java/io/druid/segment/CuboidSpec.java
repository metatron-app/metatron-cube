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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.common.guava.GuavaUtils;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.granularity.GranularityType;
import io.druid.granularity.PeriodGranularity;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CuboidSpec
{
  private final GranularityType granularity;
  private final List<String> dimensions;
  private final Map<String, Set<String>> metrics;

  @JsonCreator
  public CuboidSpec(
      @JsonProperty("granularity") GranularityType granularity,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") Map<String, Set<String>> metrics
  )
  {
    this.granularity = granularity == null ? GranularityType.ALL : granularity;
    this.dimensions = dimensions == null ? Collections.emptyList() : dimensions;
    this.metrics = metrics;
    Preconditions.checkArgument(this.granularity != GranularityType.NONE, "not supports 'NONE'");
  }

  @JsonProperty
  public GranularityType getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, Set<String>> getMetrics()
  {
    return metrics;
  }

  public boolean isApex()
  {
    return granularity == GranularityType.ALL && GuavaUtils.isNullOrEmpty(dimensions);
  }

  public int[] toDimensionIndices(List<String> mergedDimensions)
  {
    return toIndices(mergedDimensions, dimensions);
  }

  public int[] toMetricIndices(List<String> mergedMetrics)
  {
    return toIndices(mergedMetrics, metrics.keySet());
  }

  private int[] toIndices(List<String> target, Iterable<String> columns)
  {
    List<Integer> indices = Lists.newArrayList();
    for (String column : columns) {
      int index = target.indexOf(column);
      if (index >= 0) {
        indices.add(index);
      }
    }
    Collections.sort(indices);
    return Ints.toArray(indices);
  }

  public boolean supports(Set<String> columns, List<AggregatorFactory> factories, Granularity granularity)
  {
    return dimensions.size() >= columns.size() && dimensions.containsAll(columns) &&
           Cuboids.supports(metrics, factories) &&
           covers(granularity);
  }

  private boolean covers(Granularity queryGran)
  {
    if (Granularities.isAll(queryGran) || queryGran.equals(granularity.getDefaultGranularity())) {
      return true;
    }
    if (granularity == GranularityType.ALL) {
      return false;
    }
    if (queryGran instanceof PeriodGranularity) {
      PeriodGranularity periodGran = (PeriodGranularity) queryGran;
      if (periodGran.getOrigin() != null || periodGran.isCompound() || !periodGran.isUTC()) {
        return false;
      }
      // I'm not sure of this
      int index1 = Granularities.getOnlyDurationIndex(granularity.getPeriod());
      int index2 = Granularities.getOnlyDurationIndex(periodGran.getPeriod());
      if (index1 < 0 || index2 < 0 || index1 > index2) {
        return false;
      }
      if (index1 == index2) {
        int v1 = granularity.getPeriod().getValue(index1);
        int v2 = periodGran.getPeriod().getValue(index2);
        if (v1 == v2 || (v1 < v2 && v2 % v1 == 0)) {
          return true;
        }
        return false;
      }
      return true;
    }
    return false;
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
    CuboidSpec that = (CuboidSpec) o;
    return Objects.equal(granularity, that.granularity) &&
           Objects.equal(dimensions, that.dimensions) &&
           Objects.equal(metrics, that.metrics);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(granularity, dimensions, metrics);
  }

  @Override
  public String toString()
  {
    return "CuboidSpec{" +
           "granularity=" + granularity +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           '}';
  }
}
