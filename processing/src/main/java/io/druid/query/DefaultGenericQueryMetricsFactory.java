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

package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.druid.guice.LazySingleton;
import io.druid.jackson.DefaultObjectMapper;

@LazySingleton
public class DefaultGenericQueryMetricsFactory implements GenericQueryMetricsFactory
{
  private static final GenericQueryMetricsFactory INSTANCE =
      new DefaultGenericQueryMetricsFactory(new DefaultObjectMapper());

  /**
   * Should be used only in tests, directly or indirectly (e. g. in {@link
   * io.druid.query.search.SearchQueryQueryToolChest#SearchQueryQueryToolChest(io.druid.query.search.search.SearchQueryConfig)}).
   */
  @VisibleForTesting
  public static GenericQueryMetricsFactory instance()
  {
    return INSTANCE;
  }

  private final ObjectMapper jsonMapper;

  @Inject
  public DefaultGenericQueryMetricsFactory(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public QueryMetrics makeMetrics(Query<?> query)
  {
    DefaultQueryMetrics queryMetrics = new DefaultQueryMetrics(jsonMapper);
    queryMetrics.query(query);
    return queryMetrics;
  }
}
