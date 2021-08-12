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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 *
 */
public class TopNNumericResultBuilder implements TopNResultBuilder
{
  private final DateTime timestamp;
  private final DimensionSpec dimSpec;
  private final String metricName;
  private final List<PostAggregator.Processor> postAggs;
  private final MinMaxPriorityQueue<DimValHolder> pQueue;
  private final Comparator<DimValHolder> dimValComparator;
  private final String[] aggFactoryNames;

  private final int threshold;
  private final Comparator<Object> metricComparator;

  public TopNNumericResultBuilder(
      DateTime timestamp,
      DimensionSpec dimSpec,
      String metricName,
      int threshold,
      final Comparator<Object> comparator,
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    this.timestamp = timestamp;
    this.dimSpec = dimSpec;
    this.metricName = metricName;
    this.aggFactoryNames = AggregatorFactory.toNamesAsArray(aggFactories);

    this.postAggs = PostAggregators.toProcessors(PostAggregators.decorate(
        AggregatorUtil.pruneDependentPostAgg(postAggs, metricName), aggFactories
    ));

    this.threshold = threshold;
    this.metricComparator = comparator;
    this.dimValComparator = new Comparator<DimValHolder>()
    {
      @Override
      public int compare(DimValHolder d1, DimValHolder d2)
      {
        int retVal = metricComparator.compare(d1.getTopNMetricVal(), d2.getTopNMetricVal());

        if (retVal == 0) {
          retVal = -GuavaUtils.NULL_FIRST_NATURAL.compare(d1.getDimName(), d2.getDimName());
        }

        return retVal;
      }
    };

    // The logic in addEntry first adds, then removes if needed. So it can at any point have up to threshold + 1 entries.
    pQueue = MinMaxPriorityQueue.orderedBy(dimValComparator).maximumSize(threshold + 1).create();
  }

  private static final int LOOP_UNROLL_COUNT = 8;

  @Override
  public TopNNumericResultBuilder addEntry(
      Object dimName,
      Object dimValIndex,
      Object[] metricVals
  )
  {
    Preconditions.checkArgument(
        metricVals.length == aggFactoryNames.length,
        "metricVals must be the same length as aggFactories"
    );

    final Map<String, Object> metricValues = Maps.newHashMapWithExpectedSize(metricVals.length + postAggs.size() + 1);

    metricValues.put(dimSpec.getOutputName(), dimName);

    final int extra = metricVals.length % LOOP_UNROLL_COUNT;

    switch (extra) {
      case 7:
        metricValues.put(aggFactoryNames[6], metricVals[6]);
      case 6:
        metricValues.put(aggFactoryNames[5], metricVals[5]);
      case 5:
        metricValues.put(aggFactoryNames[4], metricVals[4]);
      case 4:
        metricValues.put(aggFactoryNames[3], metricVals[3]);
      case 3:
        metricValues.put(aggFactoryNames[2], metricVals[2]);
      case 2:
        metricValues.put(aggFactoryNames[1], metricVals[1]);
      case 1:
        metricValues.put(aggFactoryNames[0], metricVals[0]);
    }
    for (int i = extra; i < metricVals.length; i += LOOP_UNROLL_COUNT) {
      metricValues.put(aggFactoryNames[i + 0], metricVals[i + 0]);
      metricValues.put(aggFactoryNames[i + 1], metricVals[i + 1]);
      metricValues.put(aggFactoryNames[i + 2], metricVals[i + 2]);
      metricValues.put(aggFactoryNames[i + 3], metricVals[i + 3]);
      metricValues.put(aggFactoryNames[i + 4], metricVals[i + 4]);
      metricValues.put(aggFactoryNames[i + 5], metricVals[i + 5]);
      metricValues.put(aggFactoryNames[i + 6], metricVals[i + 6]);
      metricValues.put(aggFactoryNames[i + 7], metricVals[i + 7]);
    }

    // Order matters here, do not unroll
    for (PostAggregator.Processor postAgg : postAggs) {
      metricValues.put(postAgg.getName(), postAgg.compute(timestamp, metricValues));
    }

    Object topNMetricVal = metricValues.get(metricName);

    if (shouldAdd(topNMetricVal)) {
      DimValHolder dimValHolder = new DimValHolder.Builder()
          .withTopNMetricVal(topNMetricVal)
          .withDimName(dimName)
          .withDimValIndex(dimValIndex)
          .withMetricValues(metricValues)
          .build();
      pQueue.add(dimValHolder);
    }
    if (this.pQueue.size() > this.threshold) {
      pQueue.removeFirst();
    }

    return this;
  }

  private boolean shouldAdd(Object topNMetricVal)
  {
    final boolean belowThreshold = pQueue.size() < this.threshold;
    final boolean belowMax = belowThreshold
                             || this.metricComparator.compare(pQueue.peek().getTopNMetricVal(), topNMetricVal) < 0;
    return belowMax;
  }

  @Override
  public Result<TopNResultValue> toResult(Map<String, Map<String, Object>> events)
  {
    for (Map.Entry<String, Map<String, Object>> entry : events.entrySet()) {
      final String dimName = entry.getKey();
      final Map<String, Object> event = entry.getValue();
      final Object dimValue = event.get(metricName);
      if (shouldAdd(dimValue)) {
        pQueue.add(new DimValHolder(dimValue, dimName, null, event));
      }
      if (pQueue.size() > this.threshold) {
        pQueue.removeFirst(); // throw away
      }
    }
    return build();
  }

  @Override
  public Iterator<DimValHolder> getTopNIterator()
  {
    return pQueue.iterator();
  }

  @Override
  public Result<TopNResultValue> build()
  {
    // Pull out top aggregated values
    List<Map<String, Object>> sorted = Lists.newArrayList();
    while (!pQueue.isEmpty()) {
      sorted.add(pQueue.removeLast().getMetricValues());
    }
    return new Result<>(timestamp, new TopNResultValue(sorted));
  }
}
