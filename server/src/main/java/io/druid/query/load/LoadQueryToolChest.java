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

package io.druid.query.load;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;

import java.util.Map;

/**
 */
public class LoadQueryToolChest extends QueryToolChest<Map<String, Object>>
{
  private final GenericQueryMetricsFactory metricsFactory;

  @Inject
  public LoadQueryToolChest(
      GenericQueryMetricsFactory metricsFactory
  )
  {
    this.metricsFactory = metricsFactory;
  }

  @Override
  public QueryRunner<Map<String, Object>> mergeResults(QueryRunner<Map<String, Object>> queryRunner)
  {
    return queryRunner;
  }

  @Override
  public QueryMetrics makeMetrics(Query<Map<String, Object>> query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  public TypeReference<Map<String, Object>> getResultTypeReference(Query<Map<String, Object>> query)
  {
    return MAP_TYPE_REFERENCE;
  }
}
