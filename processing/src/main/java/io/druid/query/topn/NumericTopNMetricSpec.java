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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.Comparators;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.ordering.Direction;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;

/**
 */
public class NumericTopNMetricSpec implements TopNMetricSpec
{
  private static final byte CACHE_TYPE_ID = 0x0;

  private final String metric;
  private final Direction direction;

  @JsonCreator
  public NumericTopNMetricSpec(
      @JsonProperty("metric") String metric,
      @JsonProperty("direction") Direction direction
  )
  {
    this.metric = metric;
    this.direction = direction;
  }

  public NumericTopNMetricSpec(String metric)
  {
    this(metric, null);
  }

  @Override
  public void verifyPreconditions(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    Preconditions.checkNotNull(metric, "metric can't be null");
    Preconditions.checkNotNull(aggregatorSpecs, "aggregations cannot be null");
    Preconditions.checkArgument(
        aggregatorSpecs.size() > 0 || postAggregatorSpecs.size() > 0,
        "Must have at least one AggregatorFactory or PostAggregator"
    );

    final AggregatorFactory aggregator = Iterables.tryFind(
        aggregatorSpecs,
        new Predicate<AggregatorFactory>()
        {
          @Override
          public boolean apply(AggregatorFactory input)
          {
            return input.getName().equals(metric);
          }
        }
    ).orNull();

    final PostAggregator postAggregator = Iterables.tryFind(
        postAggregatorSpecs,
        new Predicate<PostAggregator>()
        {
          @Override
          public boolean apply(PostAggregator input)
          {
            return input.getName().equals(metric);
          }
        }
    ).orNull();

    Preconditions.checkArgument(
        aggregator != null || postAggregator != null,
        "Must have an AggregatorFactory or PostAggregator for metric[%s], gave[%s] and [%s]",
        metric,
        aggregatorSpecs,
        postAggregatorSpecs
    );
  }

  @JsonProperty
  public String getMetric()
  {
    return metric;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Direction getDirection()
  {
    return direction;
  }

  @Override
  public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    Comparator<?> comp = null;
    for (AggregatorFactory factory : aggregatorSpecs) {
      if (metric.equals(factory.getName())) {
        comp = factory.getComparator();
        break;
      }
    }
    for (PostAggregator pf : postAggregatorSpecs) {
      if (metric.equals(pf.getName())) {
        comp = pf.getComparator();
        break;
      }
    }
    if (direction == Direction.ASCENDING) {
      comp = Comparators.REVERT(comp);
    }

    return comp;
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
    return new TopNNumericResultBuilder(timestamp, dimSpec, metric, threshold, comparator, aggFactories, postAggs);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(metric);
  }

  @Override
  public <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder)
  {
    return builder;
  }

  @Override
  public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector)
  {
    selector.setAggregateTopNMetricFirst(true);
  }

  @Override
  public String getMetricName(DimensionSpec dimSpec)
  {
    return metric;
  }

  @Override
  public boolean canBeOptimizedUnordered()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "NumericTopNMetricSpec{" +
           "metric='" + metric + '\'' +
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

    NumericTopNMetricSpec that = (NumericTopNMetricSpec) o;

    if (metric != null ? !metric.equals(that.metric) : that.metric != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return metric != null ? metric.hashCode() : 0;
  }
}
