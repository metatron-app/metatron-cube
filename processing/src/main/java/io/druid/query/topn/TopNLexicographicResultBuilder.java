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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNLexicographicResultBuilder implements TopNResultBuilder
{
  private static final int LOOP_UNROLL_COUNT = 8;

  private final DateTime timestamp;
  private final DimensionSpec dimSpec;
  private final String previousStop;
  private final Comparator comparator;
  private final String[] aggFactoryNames;
  private final MinMaxPriorityQueue<DimValHolder> pQueue;
  private final int threshold;

  public TopNLexicographicResultBuilder(
      DateTime timestamp,
      DimensionSpec dimSpec,
      int threshold,
      String previousStop,
      final Comparator comparator,
      List<AggregatorFactory> aggFactories
  )
  {
    this.timestamp = timestamp;
    this.dimSpec = dimSpec;
    this.previousStop = previousStop;
    this.comparator = comparator;
    this.aggFactoryNames = AggregatorFactory.toNamesAsArray(aggFactories);
    this.threshold = threshold;

    @SuppressWarnings("unchecked")
    Comparator<DimValHolder> ordering = new Comparator<DimValHolder>()
    {
      @Override
      public int compare(DimValHolder o1, DimValHolder o2)
      {
        return -comparator.compare(o1.getDimName(), o2.getDimName());
      }
    };
    pQueue = MinMaxPriorityQueue.orderedBy(ordering).maximumSize(threshold + 1).create();
  }

  @Override
  public TopNResultBuilder addEntry(
      Comparable dimName,
      Object dimValIndex,
      Object[] metricVals
  )
  {
    final Map<String, Object> metricValues = Maps.newHashMapWithExpectedSize(metricVals.length + 1);

    if (shouldAdd(dimName)) {
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

      pQueue.add(new DimValHolder.Builder().withDimName(dimName).withMetricValues(metricValues).build());
      if (pQueue.size() > threshold) {
        pQueue.removeFirst();
      }
    }

    return this;
  }

  @Override
  public Result<TopNResultValue> toResult(Map<String, Map<String, Object>> events)
  {
    for (Map.Entry<String, Map<String, Object>> entry : events.entrySet()) {
      final String dimName = entry.getKey();
      final Map<String, Object> event = entry.getValue();

      if (shouldAdd(dimName)) {
        pQueue.add(new DimValHolder(null, dimName, null, event));
        if (pQueue.size() > threshold) {
          pQueue.removeFirst();
        }
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
    List<Map<String, Object>> sorted = Lists.newArrayList();
    while (!pQueue.isEmpty()) {
      sorted.add(pQueue.removeLast().getMetricValues());
    }
    return new Result<>(timestamp, new TopNResultValue(sorted));
  }

  private boolean shouldAdd(Comparable dimName)
  {
    final boolean belowThreshold = pQueue.size() < threshold;
    final boolean belowMax = belowThreshold
                             || comparator.compare(pQueue.peek().getTopNMetricVal(), dimName) < 0;
    // Only add if dimName is after previousStop
    return belowMax && (previousStop == null || comparator.compare(dimName, previousStop) > 0);
  }
}
