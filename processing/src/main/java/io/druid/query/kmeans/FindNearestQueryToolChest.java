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

package io.druid.query.kmeans;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.ResultMergeQueryRunner;

import java.util.Comparator;

/**
 */
public class FindNearestQueryToolChest extends QueryToolChest<CentroidDesc>
{
  private static final TypeReference<CentroidDesc> TYPE_REFERENCE =
      new TypeReference<CentroidDesc>()
      {
      };

  private final GenericQueryMetricsFactory metricsFactory;

  @Inject
  public FindNearestQueryToolChest(
      GenericQueryMetricsFactory metricsFactory
  )
  {
    this.metricsFactory = metricsFactory;
  }

  @Override
  public QueryRunner<CentroidDesc> mergeResults(
      QueryRunner<CentroidDesc> queryRunner
  )
  {
    return new ResultMergeQueryRunner<CentroidDesc>(queryRunner)
    {
      @Override
      protected Comparator<CentroidDesc> makeOrdering(Query<CentroidDesc> query)
      {
        return GuavaUtils.nullFirstNatural();
      }

      @Override
      protected BinaryFn.Identical<CentroidDesc> createMergeFn(
          Query<CentroidDesc> input
      )
      {
        return (arg1, arg2) -> arg1 == null ? arg2 : arg2 == null ? arg1 : arg1.merge(arg2);
      }
    };
  }

  @Override
  public QueryMetrics makeMetrics(Query<CentroidDesc> query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  public TypeReference<CentroidDesc> getResultTypeReference(Query<CentroidDesc> query)
  {
    return TYPE_REFERENCE;
  }
}
