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

package io.druid.query.timeseries;

import com.metamx.common.guava.nary.BinaryFn;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.Granularity;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class TimeseriesBinaryFn
    implements BinaryFn<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>, Result<TimeseriesResultValue>>
{
  private final Granularity gran;
  private final String[] metrics;
  private final AggregatorFactory.Combiner[] combiners;

  public TimeseriesBinaryFn(TimeseriesQuery timeseriesQuery)
  {
    this(timeseriesQuery.getGranularity(), timeseriesQuery.getAggregatorSpecs());
  }

  public TimeseriesBinaryFn(Granularity granularity, List<AggregatorFactory> aggregations)
  {
    this.gran = granularity;
    this.metrics = AggregatorFactory.toNamesAsArray(aggregations);
    this.combiners = AggregatorFactory.toCombinerArray(aggregations);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Result<TimeseriesResultValue> apply(Result<TimeseriesResultValue> arg1, Result<TimeseriesResultValue> arg2)
  {
    if (arg1 == null) {
      return arg2;
    }

    if (arg2 == null) {
      return arg1;
    }

    TimeseriesResultValue arg1Val = arg1.getValue();
    TimeseriesResultValue arg2Val = arg2.getValue();

    Map<String, Object> retVal = new LinkedHashMap<String, Object>();

    for (int i = 0; i < metrics.length; i++) {
      retVal.put(metrics[i], combiners[i].combine(arg1Val.getMetric(metrics[i]), arg2Val.getMetric(metrics[i])));
    }

    return (gran instanceof AllGranularity) ?
           new Result<TimeseriesResultValue>(
               arg1.getTimestamp(),
               new TimeseriesResultValue(retVal)
           ) :
           new Result<TimeseriesResultValue>(
               gran.bucketStart(arg1.getTimestamp()),
               new TimeseriesResultValue(retVal)
           );
  }

}
