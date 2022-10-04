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

package io.druid.query.search;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IdentityFunction;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValue;
import io.druid.query.CacheStrategy;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

/**
 */
public class SearchQueryQueryToolChest
    extends QueryToolChest.CacheSupport<Result<SearchResultValue>, Object, SearchQuery>
{
  private static final TypeReference<Result<SearchResultValue>> TYPE_REFERENCE = new TypeReference<Result<SearchResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };

  private final SearchQueryConfig config;
  private final GenericQueryMetricsFactory metricsFactory;

  @VisibleForTesting
  public SearchQueryQueryToolChest(SearchQueryConfig config)
  {
    this(config, DefaultGenericQueryMetricsFactory.instance());
  }

  @Inject
  public SearchQueryQueryToolChest(
      SearchQueryConfig config,
      GenericQueryMetricsFactory metricsFactory
  )
  {
    this.config = config;
    this.metricsFactory = metricsFactory;
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> mergeResults(
      QueryRunner<Result<SearchResultValue>> runner
  )
  {
    return new ResultMergeQueryRunner<Result<SearchResultValue>>(runner)
    {
      @Override
      protected Comparator<Result<SearchResultValue>> makeOrdering(Query<Result<SearchResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(query);
      }

      @Override
      protected BinaryFn.Identical<Result<SearchResultValue>> createMergeFn(
          Query<Result<SearchResultValue>> input
      )
      {
        SearchQuery query = (SearchQuery) input;
        if (query.isValueOnly()) {
          return new SearchBinaryFn(query.getSort(), query.getGranularity(), query.getLimit());
        }
        return new SearchBinaryFn.WithCount(query.getSort(), query.getGranularity(), query.getLimit());
      }
    };
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> postMergeQueryDecoration(final QueryRunner<Result<SearchResultValue>> runner)
  {
    return new QueryRunner<Result<SearchResultValue>>()
    {
      @Override
      public Sequence<Result<SearchResultValue>> run(
          Query<Result<SearchResultValue>> input, Map<String, Object> responseContext
      )
      {
        if (BaseQuery.isBySegment(input)) {
          return runner.run(input, responseContext);
        }
        SearchQuery query = (SearchQuery) input;
        Sequence<Result<SearchResultValue>> sequence = runner.run(query, responseContext);
        final Comparator<SearchHit> mergeComparator = query.getSort().getResultComparator();
        if (mergeComparator != null) {
          return Sequences.map(
              sequence, new Function<Result<SearchResultValue>, Result<SearchResultValue>>()
              {
                @Override
                public Result<SearchResultValue> apply(Result<SearchResultValue> input)
                {
                  Collections.sort(input.getValue().getValue(), mergeComparator);
                  return input;
                }
              }
          );
        }
        return sequence;
      }
    };
  }

  @Override
  public <I> QueryRunner<Result<SearchResultValue>> handleSubQuery(QuerySegmentWalker segmentWalker)
  {
    return new SearchThresholdAdjustingQueryRunner(
        new SubQueryRunner<I>(segmentWalker)
        {
          @Override
          protected Function<Interval, Sequence<Result<SearchResultValue>>> query(
              final Query<Result<SearchResultValue>> query,
              final Segment segment
          )
          {
            final SearchQuery searchQuery = (SearchQuery)query;
            final SearchQueryEngine engine = new SearchQueryEngine();
            return new Function<Interval, Sequence<Result<SearchResultValue>>>()
            {
              @Override
              public Sequence<Result<SearchResultValue>> apply(Interval interval)
              {
                return engine.process(
                    searchQuery.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)), segment, false
                );
              }
            };
          }
        }, segmentWalker.getSearchConfig()
    );
  }

  public QueryMetrics<Query<?>> makeMetrics(SearchQuery query)
  {
    return metricsFactory.makeMetrics(query).granularity(query.getGranularity());
  }

  @Override
  public TypeReference<Result<SearchResultValue>> getResultTypeReference(SearchQuery query)
  {
    return TYPE_REFERENCE;
  }

  @Override
  public JavaType getResultTypeReference(SearchQuery query, TypeFactory factory)
  {
    if (query != null && BaseQuery.isBySegment(query)) {
      return factory.constructParametricType(Result.class, BySegmentSearchResultValue.class);
    }
    return factory.constructType(getResultTypeReference(query));
  }

  @Override
  public BySegmentSearchResultValue bySegment(
      SearchQuery query,
      Sequence<Result<SearchResultValue>> sequence,
      String segmentId
  )
  {
    return new BySegmentSearchResultValue(Sequences.toList(sequence), segmentId, query.getIntervals().get(0));
  }

  @Override
  @SuppressWarnings("unchecked")
  public ToIntFunction numRows(SearchQuery query)
  {
    if (BaseQuery.isBySegment(query)) {
      return v -> ((Result<BySegmentSearchResultValue>) v).getValue().countAll();
    }
    return v -> v instanceof Result ? ((Result<SearchResultValue>) v).getValue().size() : 1;
  }

  @Override
  public CacheStrategy<Result<SearchResultValue>, Object, SearchQuery> getCacheStrategy(SearchQuery query)
  {
    return new CacheStrategy<Result<SearchResultValue>, Object, SearchQuery>()
    {
      @Override
      public byte[] computeCacheKey(SearchQuery query, int limit)
      {
        return KeyBuilder.get(limit)
                         .append(SEARCH_QUERY)
                         .append(query.getLimit())
                         .append(query.isValueOnly())
                         .append(query.getGranularity())
                         .append(query.getFilter())
                         .append(query.getSort())
                         .append(query.getVirtualColumns())
                         .append(query.getDimensions())
                         .build();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<SearchResultValue>, Object> prepareForCache()
      {
        return new Function<Result<SearchResultValue>, Object>()
        {
          @Override
          public Object apply(Result<SearchResultValue> input)
          {
            return Lists.newArrayList(input.getTimestamp().getMillis(), input.getValue());
          }
        };
      }

      @Override
      public Function<Object, Result<SearchResultValue>> pullFromCache()
      {
        return new Function<Object, Result<SearchResultValue>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Result<SearchResultValue> apply(Object input)
          {
            List<Object> result = (List<Object>) input;

            return new Result<>(
                new DateTime(((Number) result.get(0)).longValue()),
                new SearchResultValue(
                    GuavaUtils.transform(
                        (List) result.get(1),
                        new Function<Object, SearchHit>()
                        {
                          @Override
                          public SearchHit apply(@Nullable Object input)
                          {
                            if (input instanceof Map) {
                              return new SearchHit(
                                  (String) ((Map) input).get("dimension"),
                                  (String) ((Map) input).get("value"),
                                  (Integer) ((Map) input).get("count")
                              );
                            } else if (input instanceof SearchHit) {
                              return (SearchHit) input;
                            } else {
                              throw new IAE("Unknown format [%s]", input.getClass());
                            }
                          }
                        }
                    )
                )
            );
          }
        };
      }

      @Override
      public ToIntFunction<Result<SearchResultValue>> numRows(SearchQuery query)
      {
        return row -> row.getValue().size();
      }
    };
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> preMergeQueryDecoration(final QueryRunner<Result<SearchResultValue>> runner)
  {
    return new SearchThresholdAdjustingQueryRunner(runner, config);
  }

  private static class SearchThresholdAdjustingQueryRunner implements QueryRunner<Result<SearchResultValue>>
  {
    private final QueryRunner<Result<SearchResultValue>> runner;
    private final SearchQueryConfig config;

    public SearchThresholdAdjustingQueryRunner(
        QueryRunner<Result<SearchResultValue>> runner,
        SearchQueryConfig config
    )
    {
      this.runner = runner;
      this.config = config;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Sequence<Result<SearchResultValue>> run(
        Query<Result<SearchResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof SearchQuery)) {
        throw new ISE("Can only handle [%s], got [%s]", SearchQuery.class, input.getClass());
      }

      final SearchQuery query = (SearchQuery) input;
      final int maxSearchLimit = config.getMaxSearchLimit();
      if (maxSearchLimit < 0 || (query.getLimit() > 0 && query.getLimit() < maxSearchLimit)) {
        return runner.run(query, responseContext);
      }

      final Sequence sequence = runner.run(query.withLimit(maxSearchLimit), responseContext);

      if (BaseQuery.isBySegment(query)) {
        return Sequences.map(sequence, new IdentityFunction<Result<BySegmentResultValue>>()
        {
          @Override
          public Result<BySegmentResultValue> apply(Result<BySegmentResultValue> input)
          {
            BySegmentResultValue<Result<SearchResultValue>> value = input.getValue();
            return input.withValue(
                value.withTransform(result -> result.withValue(result.getValue().limit(maxSearchLimit)))
            );
          }
        });
      }

      return Sequences.map(
          (Sequence<Result<SearchResultValue>>) sequence,
          result -> result.withValue(result.getValue().limit(maxSearchLimit))
      );
    }
  }
}
