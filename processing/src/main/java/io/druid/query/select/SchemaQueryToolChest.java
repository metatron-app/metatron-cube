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

package io.druid.query.select;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.ResultMergeQueryRunner;

/**
 */
public class SchemaQueryToolChest extends QueryToolChest.CacheSupport<Schema, Schema, SchemaQuery>
{
  public static final TypeReference<Schema> TYPE_REFERENCE = new TypeReference<Schema>() {};

  private final GenericQueryMetricsFactory queryMetricsFactory;

  @Inject
  public SchemaQueryToolChest(
      GenericQueryMetricsFactory queryMetricsFactory
  )
  {
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<Schema> mergeResults(QueryRunner<Schema> runner)
  {
    return new ResultMergeQueryRunner<Schema>(runner)
    {
      @Override
      protected Ordering<Schema> makeOrdering(Query<Schema> query)
      {
        return GuavaUtils.allEquals();
      }

      @Override
      protected BinaryFn<Schema, Schema, Schema> createMergeFn(Query<Schema> input)
      {
        return new BinaryFn<Schema, Schema, Schema>()
        {
          @Override
          public Schema apply(Schema arg1, Schema arg2)
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
  public QueryMetrics<? super SchemaQuery> makeMetrics(SchemaQuery query)
  {
    return queryMetricsFactory.makeMetrics(query);
  }

  @Override
  public IdentityCacheStrategy getCacheStrategy(SchemaQuery query)
  {
    return new IdentityCacheStrategy()
    {
      @Override
      public byte[] computeCacheKey(SchemaQuery query)
      {
        return new byte[]{SCHEMA_QUERY};
      }
    };
  }

  @Override
  public TypeReference<Schema> getResultTypeReference(SchemaQuery query)
  {
    return TYPE_REFERENCE;
  }
}
