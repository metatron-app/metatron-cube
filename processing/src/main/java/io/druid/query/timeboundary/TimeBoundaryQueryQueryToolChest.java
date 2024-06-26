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

package io.druid.query.timeboundary;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.BySegmentSkippingQueryRunner;
import io.druid.query.CacheStrategy;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
public class TimeBoundaryQueryQueryToolChest
    extends QueryToolChest.CacheSupport<Result<TimeBoundaryResultValue>, Object, TimeBoundaryQuery>
{
  private static final TypeReference<Result<TimeBoundaryResultValue>> TYPE_REFERENCE = new TypeReference<Result<TimeBoundaryResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };

  private final GenericQueryMetricsFactory metricsFactory;

  @VisibleForTesting
  public TimeBoundaryQueryQueryToolChest()
  {
    this(DefaultGenericQueryMetricsFactory.instance());
  }

  @Inject
  public TimeBoundaryQueryQueryToolChest(GenericQueryMetricsFactory metricsFactory)
  {
    this.metricsFactory = metricsFactory;
  }

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(Query<Result<TimeBoundaryResultValue>> query, List<T> segments)
  {
    TimeBoundaryQuery boundary = (TimeBoundaryQuery) query;
    if (segments.size() <= 1) {
      return segments;
    }

    final T min = boundary.isMaxTime() ? null : segments.get(0);
    final T max = boundary.isMinTime() ? null : segments.get(segments.size() - 1);

    return Lists.newArrayList(
        Iterables.filter(
            segments,
            new Predicate<T>()
            {
              @Override
              public boolean apply(T input)
              {
                return (min != null && input.getInterval().overlaps(min.getInterval())) ||
                       (max != null && input.getInterval().overlaps(max.getInterval()));
              }
            }
        )
    );
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> mergeResults(
      final QueryRunner<Result<TimeBoundaryResultValue>> runner
  )
  {
    return new BySegmentSkippingQueryRunner<Result<TimeBoundaryResultValue>>(runner)
    {
      @Override
      protected Sequence<Result<TimeBoundaryResultValue>> doRun(
          QueryRunner<Result<TimeBoundaryResultValue>> baseRunner, Query<Result<TimeBoundaryResultValue>> input, Map<String, Object> context
      )
      {
        TimeBoundaryQuery query = (TimeBoundaryQuery) input;
        return Sequences.simple(
            query.mergeResults(
                Sequences.toList(baseRunner.run(query, context))
            )
        );
      }
    };
  }

  @Override
  public QueryMetrics makeMetrics(Query<Result<TimeBoundaryResultValue>> query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  public TypeReference<Result<TimeBoundaryResultValue>> getResultTypeReference(Query<Result<TimeBoundaryResultValue>> query)
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<TimeBoundaryResultValue>, Object, TimeBoundaryQuery> getCacheStrategy(TimeBoundaryQuery query)
  {
    return new CacheStrategy<Result<TimeBoundaryResultValue>, Object, TimeBoundaryQuery>()
    {
      @Override
      public byte[] computeCacheKey(TimeBoundaryQuery query, int limit)
      {
        return KeyBuilder.get(limit)
                         .append(TIMEBOUNDARY_QUERY)
                         .append(query.getBound())
                         .build();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TimeBoundaryResultValue>, Object> prepareForCache(TimeBoundaryQuery query)
      {
        return new Function<Result<TimeBoundaryResultValue>, Object>()
        {
          @Override
          public Object apply(Result<TimeBoundaryResultValue> input)
          {
            return Lists.newArrayList(input.getTimestamp().getMillis(), input.getValue());
          }
        };
      }

      @Override
      public Function<Object, Result<TimeBoundaryResultValue>> pullFromCache(TimeBoundaryQuery query)
      {
        return new Function<Object, Result<TimeBoundaryResultValue>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Result<TimeBoundaryResultValue> apply(Object input)
          {
            List<Object> result = (List<Object>) input;

            return new Result<>(
                new DateTime(((Number)result.get(0)).longValue()),
                new TimeBoundaryResultValue(result.get(1))
            );
          }
        };
      }
    };
  }
}
