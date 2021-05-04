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

package io.druid.query.timeseries;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;
import io.druid.data.input.Row;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;

/**
 */
public class HistogramQueryToolChest extends QueryToolChest<Row, HistogramQuery>
{
  private final GenericQueryMetricsFactory metricsFactory;

  @Inject
  public HistogramQueryToolChest(GenericQueryMetricsFactory metricsFactory)
  {
    this.metricsFactory = metricsFactory;
  }

  @Override
  public QueryRunner<Row> mergeResults(QueryRunner<Row> runner)
  {
    return runner;
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryMetrics makeMetrics(HistogramQuery query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  protected TypeReference<Row> getResultTypeReference(HistogramQuery query)
  {
    return ROW_TYPE_REFERENCE;
  }
}
