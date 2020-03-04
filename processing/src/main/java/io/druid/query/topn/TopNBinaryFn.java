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

import com.google.common.collect.Maps;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.Granularity;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNBinaryFn implements BinaryFn<Result<TopNResultValue>, Result<TopNResultValue>, Result<TopNResultValue>>
{
  private final TopNResultMerger merger;
  private final DimensionSpec dimSpec;
  private final Granularity gran;
  private final String dimension;
  private final TopNMetricSpec topNMetricSpec;
  private final int threshold;
  private final List<AggregatorFactory> aggregations;
  private final String[] metrics;
  private final AggregatorFactory.Combiner[] combiners;
  private final List<PostAggregator> postAggregations;
  private final List<PostAggregator.Processor> postProcessors;
  private final Comparator comparator;

  public TopNBinaryFn(
      final TopNResultMerger merger,
      final Granularity granularity,
      final DimensionSpec dimSpec,
      final TopNMetricSpec topNMetricSpec,
      final int threshold,
      final List<AggregatorFactory> aggregatorSpecs,
      final List<PostAggregator> postAggregatorSpecs
  )
  {
    this.merger = merger;
    this.dimSpec = dimSpec;
    this.gran = granularity;
    this.topNMetricSpec = topNMetricSpec;
    this.threshold = threshold;
    this.aggregations = aggregatorSpecs;
    this.metrics = AggregatorFactory.toNamesAsArray(aggregatorSpecs);
    this.combiners = AggregatorFactory.toCombinerArray(aggregatorSpecs);
    this.postAggregations = PostAggregators.decorate(
        AggregatorUtil.pruneDependentPostAgg(
            postAggregatorSpecs,
            topNMetricSpec.getMetricName(dimSpec)
        ),
        aggregations
    );
    this.postProcessors = PostAggregators.toProcessors(this.postAggregations);
    this.dimension = dimSpec.getOutputName();
    this.comparator = topNMetricSpec.getComparator(aggregatorSpecs, postAggregatorSpecs);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Result<TopNResultValue> apply(Result<TopNResultValue> arg1, Result<TopNResultValue> arg2)
  {
    if (arg1 == null) {
      return merger.getResult(arg2, comparator);
    }
    if (arg2 == null) {
      return merger.getResult(arg1, comparator);
    }

    Map<String, Map<String, Object>> retVals = Maps.newLinkedHashMap();

    TopNResultValue arg1Vals = arg1.getValue();
    TopNResultValue arg2Vals = arg2.getValue();

    for (Map<String, Object> arg1Val : arg1Vals) {
      retVals.put((String)arg1Val.get(dimension), arg1Val);
    }
    for (Map<String, Object> arg2Val : arg2Vals) {
      final String dimensionValue = (String)arg2Val.get(dimension);
      Map<String, Object> arg1Val = retVals.get(dimensionValue);

      if (arg1Val != null) {
        // size of map = aggregator + topNDim + postAgg (If sorting is done on post agg field)
        Map<String, Object> retVal = new LinkedHashMap<>(aggregations.size() + 2);

        retVal.put(dimension, dimensionValue);
        for (int i = 0; i < metrics.length; i++) {
          retVal.put(metrics[i], combiners[i].combine(arg1Val.get(metrics[i]), arg2Val.get(metrics[i])));
        }

        for (PostAggregator.Processor pf : postProcessors) {
          retVal.put(pf.getName(), pf.compute(arg1.getTimestamp(), retVal));
        }

        retVals.put(dimensionValue, retVal);
      } else {
        retVals.put(dimensionValue, arg2Val);
      }
    }

    final DateTime timestamp = gran instanceof AllGranularity
                               ? arg1.getTimestamp()
                               : gran.bucketStart(arg1.getTimestamp());

    TopNResultBuilder bob = topNMetricSpec.getResultBuilder(
        timestamp,
        dimSpec,
        threshold,
        comparator,
        aggregations,
        postAggregations
    );
    return bob.toResult(retVals);
  }
}
