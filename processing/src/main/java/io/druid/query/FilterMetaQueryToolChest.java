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
import com.google.inject.Inject;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.nary.BinaryFn;

import java.util.Map;

/**
 */
public class FilterMetaQueryToolChest extends QueryToolChest.CacheSupport<long[], long[], FilterMetaQuery>
{
  private static final TypeReference<long[]> TYPE_REFERENCE = new TypeReference<long[]>() {};

  private final GenericQueryMetricsFactory metricsFactory;

  @Inject
  public FilterMetaQueryToolChest(GenericQueryMetricsFactory metricsFactory)
  {
    this.metricsFactory = metricsFactory;
  }

  @Override
  public TypeReference<long[]> getResultTypeReference(FilterMetaQuery query)
  {
    return TYPE_REFERENCE;
  }

  @Override
  public QueryMetrics<? super FilterMetaQuery> makeMetrics(FilterMetaQuery query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  public QueryRunner<long[]> mergeResults(QueryRunner<long[]> runner)
  {
    return new ResultMergeQueryRunner.MergeAll<long[]>(runner)
    {
      @Override
      protected BinaryFn.Identical<long[]> createMergeFn(Query<long[]> input)
      {
        return (arg1, arg2) -> new long[] {arg1[0] + arg2[0], arg1[1] + arg2[1]};
      }

      @Override
      public Sequence<long[]> doRun(QueryRunner<long[]> baseRunner, Query<long[]> query, Map<String, Object> context)
      {
        return Sequences.filterNull(super.doRun(baseRunner, query, context));
      }
    };
  }

  @Override
  public IdenticalCacheStrategy getCacheStrategy(final FilterMetaQuery query)
  {
    return new IdenticalCacheStrategy()
    {
      @Override
      public byte[] computeCacheKey(FilterMetaQuery query, int limit)
      {
        return KeyBuilder.get(limit)
                         .append(FILTER_META_QUERY)
                         .append(query.getFilter())
                         .append(query.getVirtualColumns())
                         .build();
      }
    };
  }
}
