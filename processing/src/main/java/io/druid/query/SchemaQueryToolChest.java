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

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.inject.Inject;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.nary.BinaryFn;

import java.util.Comparator;
import java.util.List;

/**
 */
public class SchemaQueryToolChest extends QueryToolChest.CacheSupport<Schema, Schema, SchemaQuery>
{
  public static final TypeReference<Schema> TYPE_REFERENCE = new TypeReference<Schema>() {};

  private final GenericQueryMetricsFactory metricsFactory;

  @Inject
  public SchemaQueryToolChest(
      GenericQueryMetricsFactory metricsFactory
  )
  {
    this.metricsFactory = metricsFactory;
  }

  @Override
  public QueryRunner<Schema> mergeResults(QueryRunner<Schema> runner)
  {
    return new ResultMergeQueryRunner<Schema>(runner)
    {
      @Override
      protected Comparator<Schema> makeOrdering(Query<Schema> query)
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
    return metricsFactory.makeMetrics(query);
  }

  @Override
  public JavaType getResultTypeReference(SchemaQuery query, TypeFactory factory)
  {
    if (query != null && BaseQuery.isBySegment(query)) {
      return factory.constructParametricType(Result.class, BySegmentSchemaValue.class);
    }
    return factory.constructType(getResultTypeReference(query));
  }

  @Override
  public BySegmentResultValue<Schema> bySegment(SchemaQuery query, Sequence<Schema> sequence, String segmentId)
  {
    // realtime node can return multiple schemas for single segment range.. fxxx
    List<Schema> schemas = Sequences.toList(sequence);
    Schema schema = schemas.get(0);
    for (int i = 1; i < schemas.size(); i++) {
      schema = schema.merge(schemas.get(i));
    }
    return new BySegmentSchemaValue(schema, segmentId, query.getIntervals().get(0));
  }

  @Override
  public IdenticalCacheStrategy getCacheStrategy(SchemaQuery query)
  {
    return new IdenticalCacheStrategy()
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
