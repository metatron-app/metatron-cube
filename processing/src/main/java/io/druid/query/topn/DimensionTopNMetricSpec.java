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

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.common.KeyBuilder;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.ordering.StringComparators;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;

/**
 */
public class DimensionTopNMetricSpec implements TopNMetricSpec
{
  private static final byte STRING_SEPARATOR = (byte) 0xFF;

  private static final byte CACHE_TYPE_ID = 0x4;
  private final String previousStop;
  private final String ordering;

  @JsonCreator
  public DimensionTopNMetricSpec(
      @JsonProperty("previousStop") String previousStop,
      @JsonProperty("ordering") String ordering
  )
  {
    this.previousStop = previousStop;
    this.ordering = ordering == null ? StringComparators.LEXICOGRAPHIC_NAME : ordering;
  }

  @Override
  public void verifyPreconditions(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
  }

  @JsonProperty
  public String getPreviousStop()
  {
    return previousStop;
  }

  @JsonProperty
  public String getOrdering()
  {
    return ordering;
  }

  @Override
  public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    return StringComparators.makeComparator(ordering);
  }

  @Override
  public TopNResultBuilder getResultBuilder(
      DateTime timestamp,
      DimensionSpec dimSpec,
      int threshold,
      Comparator comparator,
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    return new TopNLexicographicResultBuilder(
        timestamp,
        dimSpec,
        threshold,
        previousStop,
        comparator,
        aggFactories
    );
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(previousStop).sp()
                  .append(ordering);
  }

  @Override
  public <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder)
  {
    if (StringComparators.isLexicographicString(ordering)) {
      builder.skipTo(previousStop);
      builder.ignoreAfterThreshold();
    }
    return builder;
  }

  @Override
  public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector)
  {
    selector.setAggregateAllMetrics(true);
  }

  @Override
  public String getMetricName(DimensionSpec dimSpec)
  {
    return dimSpec.getOutputName();
  }

  @Override
  public boolean canBeOptimizedUnordered()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "DimensionTopNMetricSpec{" +
           "previousStop='" + previousStop + '\'' +
           ", ordering=" + ordering +
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

    DimensionTopNMetricSpec that = (DimensionTopNMetricSpec) o;

    if (getPreviousStop() != null
        ? !getPreviousStop().equals(that.getPreviousStop())
        : that.getPreviousStop() != null) {
      return false;
    }
    return getOrdering().equals(that.getOrdering());

  }

  @Override
  public int hashCode()
  {
    int result = getPreviousStop() != null ? getPreviousStop().hashCode() : 0;
    result = 31 * result + getOrdering().hashCode();
    return result;
  }
}
