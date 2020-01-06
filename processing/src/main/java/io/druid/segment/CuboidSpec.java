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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupByQuery;
import io.druid.segment.filter.Filters;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CuboidSpec
{
  private final Granularity granularity;  // todo
  private final List<String> dimensions;
  private final Map<String, Set<String>> metrics;

  @JsonCreator
  public CuboidSpec(
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") Map<String, Set<String>> metrics
  )
  {
    this.granularity = granularity == null ? Granularities.ALL : granularity;
    this.dimensions = dimensions;
    this.metrics = metrics;
  }

  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public Map<String, Set<String>> getMetrics()
  {
    return metrics;
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

  public boolean supports(GroupByQuery query)
  {
    return Objects.equal(granularity, query.getGranularity()) &&
           dimensions.containsAll(DimensionSpecs.toInputNames(query.getDimensions())) &&
           dimensions.containsAll(Filters.getDependents(query.getFilter())) &&
           Cuboids.supports(metrics, query.getAggregatorSpecs());
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
