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
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.ResultMergeQueryRunner;

/**
 */
public class FindNearestQueryToolChest extends QueryToolChest<CentroidDesc, FindNearestQuery>
{
  private static final TypeReference<CentroidDesc> TYPE_REFERENCE =
      new TypeReference<CentroidDesc>()
      {
      };

  private final GenericQueryMetricsFactory queryMetricsFactory;

  @Inject
  public FindNearestQueryToolChest(
      GenericQueryMetricsFactory queryMetricsFactory
  )
  {
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<CentroidDesc> mergeResults(
      QueryRunner<CentroidDesc> queryRunner
  )
  {
    return new ResultMergeQueryRunner<CentroidDesc>(queryRunner)
    {
      @Override
      protected Ordering<CentroidDesc> makeOrdering(Query<CentroidDesc> query)
      {
        return Ordering.natural();
      }

      @Override
      protected BinaryFn<CentroidDesc, CentroidDesc, CentroidDesc> createMergeFn(
          Query<CentroidDesc> input
      )
      {
        return new BinaryFn<CentroidDesc, CentroidDesc, CentroidDesc>()
        {
          @Override
          public CentroidDesc apply(CentroidDesc arg1, CentroidDesc arg2)
          {
            if (arg1 == null) {
              return arg2;
            }
            if (arg2 == null) {
              return arg1;
            }
            return arg1.merge(arg2);
          }
        };
      }
    };
  }

  @Override
  public QueryMetrics<? super FindNearestQuery> makeMetrics(FindNearestQuery query)
  {
    return queryMetricsFactory.makeMetrics(query);
  }

  @Override
  public TypeReference<CentroidDesc> getResultTypeReference(FindNearestQuery query)
  {
    return TYPE_REFERENCE;
  }
}
