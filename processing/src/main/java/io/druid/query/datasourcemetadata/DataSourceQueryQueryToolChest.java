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

package io.druid.query.datasourcemetadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.BySegmentSkippingQueryRunner;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.timeline.LogicalSegment;

import java.util.List;
import java.util.Map;

/**
 */
public class DataSourceQueryQueryToolChest
    extends QueryToolChest<Result<DataSourceMetadataResultValue>, DataSourceMetadataQuery>
{
  private static final TypeReference<Result<DataSourceMetadataResultValue>> TYPE_REFERENCE = new TypeReference<Result<DataSourceMetadataResultValue>>()
  {
  };

  private final GenericQueryMetricsFactory metricsFactory;

  @Inject
  public DataSourceQueryQueryToolChest(GenericQueryMetricsFactory metricsFactory)
  {
    this.metricsFactory = metricsFactory;
  }

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(DataSourceMetadataQuery query, List<T> segments)
  {
    if (segments.size() <= 1) {
      return segments;
    }

    final T max = segments.get(segments.size() - 1);

    return Lists.newArrayList(
        Iterables.filter(
            segments,
            new Predicate<T>()
            {
              @Override
              public boolean apply(T input)
              {
                return max != null && input.getInterval().overlaps(max.getInterval());
              }
            }
        )
    );
  }

  @Override
  public QueryRunner<Result<DataSourceMetadataResultValue>> mergeResults(
      final QueryRunner<Result<DataSourceMetadataResultValue>> runner
  )
  {
    return new BySegmentSkippingQueryRunner<Result<DataSourceMetadataResultValue>>(runner)
    {
      @Override
      protected Sequence<Result<DataSourceMetadataResultValue>> doRun(
          QueryRunner<Result<DataSourceMetadataResultValue>> baseRunner,
          Query<Result<DataSourceMetadataResultValue>> input,
          Map<String, Object> context
      )
      {
        DataSourceMetadataQuery query = (DataSourceMetadataQuery) input;
        return Sequences.simple(
            query.mergeResults(
                Sequences.toList(
                    baseRunner.run(query, context)
                )
            )
        );
      }
    };
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(DataSourceMetadataQuery query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  public TypeReference<Result<DataSourceMetadataResultValue>> getResultTypeReference(DataSourceMetadataQuery query)
  {
    return TYPE_REFERENCE;
  }
}
