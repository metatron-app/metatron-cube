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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.query.DefaultQueryMetrics;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;

public class DefaultTimeseriesQueryMetrics extends DefaultQueryMetrics
    implements TimeseriesQueryMetrics
{
  public DefaultTimeseriesQueryMetrics(ObjectMapper jsonMapper)
  {
    super(jsonMapper);
  }

  @Override
  public void query(Query<?> query)
  {
    TimeseriesQuery timeseries = (TimeseriesQuery) query;
    super.query(timeseries);
    numMetrics(timeseries);
    numComplexMetrics(timeseries);
  }

  @Override
  public void numMetrics(TimeseriesQuery query)
  {
    builder.setDimension("numMetrics", String.valueOf(query.getAggregatorSpecs().size()));
  }

  @Override
  public void numComplexMetrics(TimeseriesQuery query)
  {
    int numComplexAggs = DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs());
    builder.setDimension("numComplexMetrics", String.valueOf(numComplexAggs));
  }
}
