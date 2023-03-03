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
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.common.KeyBuilder;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
public class SelectMetaQueryToolChest
    extends QueryToolChest.CacheSupport<Result<SelectMetaResultValue>, Result<SelectMetaResultValue>, SelectMetaQuery>
{
  private static final TypeReference<Result<SelectMetaResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<SelectMetaResultValue>>()
      {
      };

  private final GenericQueryMetricsFactory metricsFactory;

  @Inject
  public SelectMetaQueryToolChest(
      GenericQueryMetricsFactory metricsFactory
  )
  {
    this.metricsFactory = metricsFactory;
  }
  @Override
  public QueryRunner<Result<SelectMetaResultValue>> mergeResults(QueryRunner<Result<SelectMetaResultValue>> runner)
  {
    return new ResultMergeQueryRunner<Result<SelectMetaResultValue>>(runner)
    {
      @Override
      protected Comparator<Result<SelectMetaResultValue>> makeOrdering(Query<Result<SelectMetaResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(query);
      }

      @Override
      protected BinaryFn.Identical<Result<SelectMetaResultValue>> createMergeFn(
          Query<Result<SelectMetaResultValue>> input
      )
      {
        SelectMetaQuery query = (SelectMetaQuery) input;
        final Granularity gran = query.getGranularity();
        return new BinaryFn.Identical<Result<SelectMetaResultValue>>()
        {
          @Override
          public Result<SelectMetaResultValue> apply(
              Result<SelectMetaResultValue> arg1, Result<SelectMetaResultValue> arg2
          )
          {
            if (arg1 == null) {
              return arg2;
            }
            if (arg2 == null) {
              return arg1;
            }
            final DateTime timestamp = gran instanceof AllGranularity
                                       ? arg1.getTimestamp()
                                       : gran.bucketStart(arg1.getTimestamp());
            SelectMetaResultValue value1 = arg1.getValue();
            SelectMetaResultValue value2 = arg2.getValue();

            Map<String, Integer> merged = Maps.newTreeMap();
            merged.putAll(value1.getPerSegmentCounts());
            for (Map.Entry<String, Integer> entry : value2.getPerSegmentCounts().entrySet()) {
              Integer prev = merged.get(entry.getKey());
              merged.put(entry.getKey(), prev == null ? entry.getValue() : prev + entry.getValue());
            }
            return new Result<>(timestamp, new SelectMetaResultValue(merged));
          }
        };
      }
    };
  }

  @Override
  public QueryMetrics makeMetrics(Query<Result<SelectMetaResultValue>> query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  public IdenticalCacheStrategy<SelectMetaQuery> getCacheStrategy(SelectMetaQuery query)
  {
    if (query.getPagingSpec() != null) {
      return null;
    }
    return new IdenticalCacheStrategy<SelectMetaQuery>()
    {
      @Override
      public byte[] computeCacheKey(SelectMetaQuery query, int limit)
      {
        return KeyBuilder.get(limit)
                         .append(SELECT_META_QUERY)
                         .append(query.getFilter())
                         .append(query.getGranularity())
                         .append(query.getVirtualColumns())
                         .append(query.getDimensions())
                         .append(query.getMetrics())
                         .build();
      }
    };
  }

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(Query<Result<SelectMetaResultValue>> query, List<T> segments)
  {
    // shares same logic
    return SelectQueryQueryToolChest.filterSegmentsOnPagingSpec(((SelectMetaQuery) query).toBaseQuery(), segments);
  }

  @Override
  public TypeReference<Result<SelectMetaResultValue>> getResultTypeReference(Query<Result<SelectMetaResultValue>> query)
  {
    return TYPE_REFERENCE;
  }
}
