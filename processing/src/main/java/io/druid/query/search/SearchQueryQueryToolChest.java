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

package io.druid.query.search;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.query.BaseQuery;
import io.druid.query.CacheStrategy;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class SearchQueryQueryToolChest extends QueryToolChest<Result<SearchResultValue>, SearchQuery>
{
  private static final TypeReference<Result<SearchResultValue>> TYPE_REFERENCE = new TypeReference<Result<SearchResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };

  private final Supplier<SearchQueryConfig> config;

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public SearchQueryQueryToolChest(
      Supplier<SearchQueryConfig> config,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.config = config;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> mergeResults(
      QueryRunner<Result<SearchResultValue>> runner
  )
  {
    return new ResultMergeQueryRunner<Result<SearchResultValue>>(runner)
    {
      @Override
      protected Ordering<Result<SearchResultValue>> makeOrdering(Query<Result<SearchResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(
            ((SearchQuery) query).getGranularity(),
            query.isDescending()
        );
      }

      @Override
      protected BinaryFn<Result<SearchResultValue>, Result<SearchResultValue>, Result<SearchResultValue>> createMergeFn(
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
  public <I> QueryRunner<Result<SearchResultValue>> handleSubQuery(
      final QueryRunner<I> subQueryRunner,
      final QuerySegmentWalker segmentWalker,
      final ExecutorService executor,
      final int maxRowCount
  )
  {
    return new SearchThresholdAdjustingQueryRunner(
        new SubQueryRunner<I>(subQueryRunner, segmentWalker, executor, maxRowCount)
        {
          @Override
          protected Function<Interval, Sequence<Result<SearchResultValue>>> function(
              final Query<Result<SearchResultValue>> query,
              final Map<String, Object> context,
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
        }, config.get()
    );
  }

  @Override
  public TypeReference<Result<SearchResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<SearchResultValue>, Object, SearchQuery> getCacheStrategy(SearchQuery query)
  {
    return new CacheStrategy<Result<SearchResultValue>, Object, SearchQuery>()
    {
      @Override
      public byte[] computeCacheKey(SearchQuery query)
      {
        final DimFilter dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] querySpecBytes = query.getQuery().getCacheKey();
        final byte[] granularityBytes = query.getGranularity().getCacheKey();

        final byte[] vcBytes = QueryCacheHelper.computeCacheKeys(query.getVirtualColumns());
        final Collection<DimensionSpec> dimensions = query.getDimensions() == null
                                                     ? ImmutableList.<DimensionSpec>of()
                                                     : query.getDimensions();

        final byte[][] dimensionsBytes = new byte[dimensions.size()][];
        int dimensionsBytesSize = 0;
        int index = 0;
        for (DimensionSpec dimension : dimensions) {
          dimensionsBytes[index] = dimension.getCacheKey();
          dimensionsBytesSize += dimensionsBytes[index].length;
          ++index;
        }

        final byte[] sortSpecBytes = query.getSort().getCacheKey();

        final ByteBuffer queryCacheKey = ByteBuffer
            .allocate(
                1 + 4 + 1 + granularityBytes.length + filterBytes.length +
                querySpecBytes.length + vcBytes.length + dimensionsBytesSize + sortSpecBytes.length
            )
            .put(SEARCH_QUERY)
            .put(Ints.toByteArray(query.getLimit()))
            .put(query.isValueOnly() ? (byte)0x01 : 0)
            .put(granularityBytes)
            .put(filterBytes)
            .put(querySpecBytes)
            .put(sortSpecBytes)
            .put(vcBytes)
            ;

        for (byte[] bytes : dimensionsBytes) {
          queryCacheKey.put(bytes);
        }

        return queryCacheKey.array();
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
                    Lists.newArrayList(
                        Lists.transform(
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
                )
            );
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> preMergeQueryDecoration(final QueryRunner<Result<SearchResultValue>> runner)
  {
    return new SearchThresholdAdjustingQueryRunner(
        intervalChunkingQueryRunnerDecorator.decorate(
            new QueryRunner<Result<SearchResultValue>>()
            {
              @Override
              public Sequence<Result<SearchResultValue>> run(
                  Query<Result<SearchResultValue>> query, Map<String, Object> responseContext
              )
              {
                SearchQuery searchQuery = (SearchQuery) query;
                if (searchQuery.getDimensionsFilter() != null) {
                  searchQuery = searchQuery.withDimFilter(searchQuery.getDimensionsFilter().optimize());
                }
                return runner.run(searchQuery, responseContext);
              }
            } , this),
        config.get()
    );
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

      final boolean isBySegment = BaseQuery.getContextBySegment(query, false);

      return Sequences.map(
          runner.run(query.withLimit(maxSearchLimit), responseContext),
          new Function<Result<SearchResultValue>, Result<SearchResultValue>>()
          {
            @Override
            public Result<SearchResultValue> apply(Result<SearchResultValue> input)
            {
              if (isBySegment) {
                BySegmentSearchResultValue value = (BySegmentSearchResultValue) input.getValue();

                return new Result<SearchResultValue>(
                    input.getTimestamp(),
                    new BySegmentSearchResultValue(
                        Lists.transform(
                            value.getResults(),
                            new Function<Result<SearchResultValue>, Result<SearchResultValue>>()
                            {
                              @Override
                              public Result<SearchResultValue> apply(@Nullable Result<SearchResultValue> input)
                              {
                                return new Result<SearchResultValue>(
                                    input.getTimestamp(),
                                    new SearchResultValue(
                                        Lists.newArrayList(
                                            Iterables.limit(
                                                input.getValue(),
                                                maxSearchLimit
                                            )
                                        )
                                    )
                                );
                              }
                            }
                        ),
                        value.getSegmentId(),
                        value.getInterval()
                    )
                );
              }

              return new Result<SearchResultValue>(
                  input.getTimestamp(),
                  new SearchResultValue(
                      Lists.<SearchHit>newArrayList(
                          Iterables.limit(input.getValue(), maxSearchLimit)
                      )
                  )
              );
            }
          }
      );
    }
  }
}
