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

package io.druid.query.topn;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.CombiningSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IdentityFunction;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValue;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.CacheStrategy;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

/**
 */
public class TopNQueryQueryToolChest
    extends QueryToolChest.CacheSupport<Result<TopNResultValue>, List<Object>, TopNQuery>
{
  private static final TypeReference<Result<TopNResultValue>> TYPE_REFERENCE = new TypeReference<Result<TopNResultValue>>()
  {
  };
  private static final TypeReference<List<Object>> OBJECT_TYPE_REFERENCE = new TypeReference<List<Object>>()
  {
  };

  private final TopNQueryConfig config;
  private final TopNQueryEngine engine;
  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;
  private final TopNQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public TopNQueryQueryToolChest(
      TopNQueryConfig config,
      TopNQueryEngine engine,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this(config, engine, intervalChunkingQueryRunnerDecorator, DefaultTopNQueryMetricsFactory.instance());
  }

  @Inject
  public TopNQueryQueryToolChest(
      TopNQueryConfig config,
      TopNQueryEngine engine,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator,
      TopNQueryMetricsFactory queryMetricsFactory
  )
  {
    this.config = config;
    this.engine = engine;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  private static List<PostAggregator> prunePostAggregators(TopNQuery query)
  {
    return AggregatorUtil.pruneDependentPostAgg(
        query.getPostAggregatorSpecs(),
        query.getTopNMetricSpec().getMetricName(query.getDimensionSpec())
    );
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> mergeResults(final QueryRunner<Result<TopNResultValue>> runner)
  {
    return new QueryRunner<Result<TopNResultValue>>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Result<TopNResultValue>> run(
          Query<Result<TopNResultValue>> query,
          Map<String, Object> responseContext
      )
      {
        TopNQuery topN = (TopNQuery) query;
        if (topN.getContextBoolean(QueryContextKeys.FINAL_MERGE, true)) {
          Sequence<Result<TopNResultValue>> sequence = runner.run(topN.toLocalQuery(), responseContext);
          if (BaseQuery.isBySegment(topN)) {
            Function function = BySegmentResultValueClass.applyAll(toPostAggregator(topN));
            return Sequences.map(sequence, function);
          }
          TopNBinaryFn topNBinaryFn = new TopNBinaryFn(
              TopNResultMerger.identity,
              topN.getGranularity(),
              topN.getDimensionSpec(),
              topN.getTopNMetricSpec(),
              topN.getThreshold(),
              topN.getAggregatorSpecs(),
              topN.getPostAggregatorSpecs()
          );
          sequence = CombiningSequence.create(sequence, ResultGranularTimestampComparator.create(topN), topNBinaryFn);
          sequence = Sequences.map(sequence, toPostAggregator(topN));
          return sequence;
        }
        return runner.run(topN, responseContext);
      }
    };
  }

  @SuppressWarnings("unchecked")
  private IdentityFunction<Result<TopNResultValue>> toPostAggregator(TopNQuery topN)
  {
    if (GuavaUtils.isNullOrEmpty(topN.getPostAggregatorSpecs())) {
      return IdentityFunction.INSTANCE;
    }
    final List<PostAggregator> postAggregators = PostAggregators.decorate(
        topN.getPostAggregatorSpecs(),
        topN.getAggregatorSpecs()
    );

    return new IdentityFunction<Result<TopNResultValue>>()
    {
      @Override
      public Result<TopNResultValue> apply(Result<TopNResultValue> input)
      {
        final DateTime timestamp = input.getTimestamp();
        final TopNResultValue holder = input.getValue();
        return new Result<TopNResultValue>(
            timestamp,
            new TopNResultValue(Lists.transform(
                holder.getValue(),
                new Function<Map<String, Object>, Map<String, Object>>()
                {

                  @Override
                  public Map<String, Object> apply(Map<String, Object> input)
                  {
                    for (PostAggregator postAgg : postAggregators) {
                      input.put(postAgg.getName(), postAgg.compute(timestamp, input));
                    }
                    return input;
                  }
                }
            ))
        );
      }
    };
  }

  @Override
  public TopNQueryMetrics makeMetrics(TopNQuery query)
  {
    TopNQueryMetrics queryMetrics = queryMetricsFactory.makeMetrics();
    queryMetrics.query(query);
    return queryMetrics;
  }

  @Override
  public Function<Result<TopNResultValue>, Result<TopNResultValue>> makePreComputeManipulatorFn(
      final TopNQuery query, final MetricManipulationFn fn
  )
  {
    return makeComputeManipulatorFn(query, fn);
  }

  @Override
  public Function<Result<TopNResultValue>, Result<TopNResultValue>> makePostComputeManipulatorFn(
      final TopNQuery query, final MetricManipulationFn fn
  )
  {
    return makeComputeManipulatorFn(query, fn);
  }

  private Function<Result<TopNResultValue>, Result<TopNResultValue>> makeComputeManipulatorFn(
      final TopNQuery query,
      final MetricManipulationFn fn
  )
  {
    if (fn == MetricManipulatorFns.identity() || GuavaUtils.isNullOrEmpty(query.getAggregatorSpecs())) {
      return Functions.identity();
    }
    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      @Override
      public Result<TopNResultValue> apply(Result<TopNResultValue> result)
      {
        final DateTime timestamp = result.getTimestamp();
        final TopNResultValue holder = result.getValue();
        return new Result<TopNResultValue>(
            timestamp,
            new TopNResultValue(Lists.transform(
                holder.getValue(),
                new Function<Map<String, Object>, Map<String, Object>>()
                {

                  @Override
                  public Map<String, Object> apply(Map<String, Object> input)
                  {
                    for (AggregatorFactory agg : query.getAggregatorSpecs()) {
                      input.put(agg.getName(), fn.manipulate(agg, input.get(agg.getName())));
                    }
                    return input;
                  }
                }
            ))
        );
      }
    };
  }

  @Override
  public TypeReference<Result<TopNResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ToIntFunction numRows(TopNQuery query)
  {
    if (BaseQuery.isBySegment(query)) {
      return new ToIntFunction()
      {
        @Override
        public int applyAsInt(Object bySegment)
        {
          int counter = 0;
          for (Object value : BySegmentResultValueClass.unwrap(bySegment)) {
            counter += ((Result<TopNResultValue>) value).getValue().size();
          }
          return counter;
        }
      };
    }
    return new ToIntFunction()
    {
      @Override
      public int applyAsInt(Object value)
      {
        if (value instanceof Result) {
          return ((Result<TopNResultValue>) value).getValue().size();
        }
        return 1;
      }
    };
  }

  @Override
  public CacheStrategy<Result<TopNResultValue>, List<Object>, TopNQuery> getCacheStrategy(final TopNQuery query)
  {
    return new CacheStrategy<Result<TopNResultValue>, List<Object>, TopNQuery>()
    {
      private final List<AggregatorFactory> aggs = Lists.newArrayList(query.getAggregatorSpecs());
      private final List<PostAggregator> postAggs = PostAggregators.decorate(
          AggregatorUtil.pruneDependentPostAgg(
              query.getPostAggregatorSpecs(),
              query.getTopNMetricSpec()
                   .getMetricName(query.getDimensionSpec())
          ),
          query.getAggregatorSpecs()
      );

      @Override
      public byte[] computeCacheKey(TopNQuery query)
      {
        return KeyBuilder.get()
                         .append(TOPN_QUERY)
                         .append(query.getVirtualColumns())
                         .append(query.getDimensionSpec())
                         .append(query.getTopNMetricSpec())
                         .append(query.getThreshold())
                         .append(query.getGranularity())
                         .append(query.getFilter())
                         .append(query.getAggregatorSpecs())
                         .build();
      }

      @Override
      public TypeReference<List<Object>> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TopNResultValue>, List<Object>> prepareForCache()
      {
        return new Function<Result<TopNResultValue>, List<Object>>()
        {
          private final String[] aggFactoryNames = AggregatorFactory.toNamesAsArray(query.getAggregatorSpecs());

          @Override
          public List<Object> apply(final Result<TopNResultValue> input)
          {
            List<Map<String, Object>> results = Lists.newArrayList(input.getValue());
            final List<Object> retVal = Lists.newArrayListWithCapacity(results.size() + 1);

            // make sure to preserve timezone information when caching results
            retVal.add(input.getTimestamp().getMillis());
            for (Map<String, Object> result : results) {
              List<Object> vals = Lists.newArrayListWithCapacity(aggFactoryNames.length + 2);
              vals.add(result.get(query.getDimensionSpec().getOutputName()));
              for (String aggName : aggFactoryNames) {
                vals.add(result.get(aggName));
              }
              retVal.add(vals);
            }
            return retVal;
          }
        };
      }

      @Override
      public Function<List<Object>, Result<TopNResultValue>> pullFromCache()
      {
        return new Function<List<Object>, Result<TopNResultValue>>()
        {
          private final Granularity granularity = query.getGranularity();

          @Override
          @SuppressWarnings("unchecked")
          public Result<TopNResultValue> apply(List<Object> results)
          {
            List<Map<String, Object>> retVal = Lists.newArrayListWithCapacity(results.size());

            Iterator<Object> inputIter = results.iterator();
            DateTime timestamp = granularity.toDateTime(((Number) inputIter.next()).longValue());

            while (inputIter.hasNext()) {
              List<Object> result = (List<Object>) inputIter.next();
              Map<String, Object> vals = Maps.newLinkedHashMap();

              Iterator<AggregatorFactory> aggIter = aggs.iterator();
              Iterator<Object> resultIter = result.iterator();

              vals.put(query.getDimensionSpec().getOutputName(), resultIter.next());

              while (aggIter.hasNext() && resultIter.hasNext()) {
                final AggregatorFactory factory = aggIter.next();
                vals.put(factory.getName(), factory.deserialize(resultIter.next()));
              }

              for (PostAggregator postAgg : postAggs) {
                vals.put(postAgg.getName(), postAgg.compute(timestamp, vals));
              }

              retVal.add(vals);
            }

            return new Result<>(timestamp, new TopNResultValue(retVal));
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> preMergeQueryDecoration(final QueryRunner<Result<TopNResultValue>> runner)
  {
    return intervalChunkingQueryRunnerDecorator.decorate(
        new QueryRunner<Result<TopNResultValue>>()
        {
          @Override
          public Sequence<Result<TopNResultValue>> run(
              Query<Result<TopNResultValue>> query, Map<String, Object> responseContext
          )
          {
            TopNQuery topNQuery = (TopNQuery) query;
            if (TopNQueryEngine.canApplyExtractionInPost(topNQuery)) {
              DimensionSpec dimensionSpec = topNQuery.getDimensionSpec();
              topNQuery = topNQuery.withDimensionSpec(
                  new DefaultDimensionSpec(
                      dimensionSpec.getDimension(),
                      dimensionSpec.getOutputName()
                  )
              );
            }
            return runner.run(topNQuery, responseContext);
          }
        }
        , this
    );
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> postMergeQueryDecoration(final QueryRunner<Result<TopNResultValue>> runner)
  {
    final ThresholdAdjustingQueryRunner thresholdRunner = new ThresholdAdjustingQueryRunner(
        runner,
        config
    );
    return new QueryRunner<Result<TopNResultValue>>()
    {

      @Override
      public Sequence<Result<TopNResultValue>> run(
          final Query<Result<TopNResultValue>> query, final Map<String, Object> responseContext
      )
      {
        // thresholdRunner.run throws ISE if query is not TopNQuery
        final Sequence<Result<TopNResultValue>> resultSequence = thresholdRunner.run(query, responseContext);
        final TopNQuery topNQuery = (TopNQuery) query;
        if (!TopNQueryEngine.canApplyExtractionInPost(topNQuery)) {
          return resultSequence;
        } else {
          final DimensionSpec dimensionSpec = topNQuery.getDimensionSpec();
          final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();
          final String dimOutputName = dimensionSpec.getOutputName();
          return Sequences.map(
              resultSequence, new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
              {
                @Override
                public Result<TopNResultValue> apply(Result<TopNResultValue> input)
                {
                  TopNResultValue resultValue = input.getValue();

                  return new Result<TopNResultValue>(
                      input.getTimestamp(),
                      new TopNResultValue(Lists.transform(
                          resultValue.getValue(),
                          new Function<Map<String, Object>, Map<String, Object>>()
                          {
                            @Override
                            public Map<String, Object> apply(Map<String, Object> input)
                            {
                              input.put(dimOutputName, extractionFn.apply(input.get(dimOutputName)));
                              return input;
                            }
                          }
                      ))
                  );
                }
              }
          );
        }
      }
    };
  }

  static class ThresholdAdjustingQueryRunner implements QueryRunner<Result<TopNResultValue>>
  {
    private final QueryRunner<Result<TopNResultValue>> runner;
    private final TopNQueryConfig config;

    public ThresholdAdjustingQueryRunner(
        QueryRunner<Result<TopNResultValue>> runner,
        TopNQueryConfig config
    )
    {
      this.runner = runner;
      this.config = config;
    }

    @Override
    public Sequence<Result<TopNResultValue>> run(
        Query<Result<TopNResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof TopNQuery)) {
        throw new ISE("Can only handle [%s], got [%s]", TopNQuery.class, input.getClass());
      }

      final TopNQuery query = (TopNQuery) input;
      final int minTopNThreshold = query.getContextValue("minTopNThreshold", config.getMinTopNThreshold());
      if (query.getThreshold() > minTopNThreshold) {
        return runner.run(query, responseContext);
      }

      final boolean isBySegment = BaseQuery.isBySegment(query);

      return Sequences.map(
          runner.run(query.withThreshold(minTopNThreshold), responseContext),
          new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
          {
            @Override
            public Result<TopNResultValue> apply(Result<TopNResultValue> input)
            {
              if (isBySegment) {
                BySegmentResultValue<Result<TopNResultValue>> value = (BySegmentResultValue<Result<TopNResultValue>>) input
                    .getValue();

                return new Result<TopNResultValue>(
                    input.getTimestamp(),
                    new BySegmentTopNResultValue(
                        Lists.transform(
                            value.getResults(),
                            new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
                            {
                              @Override
                              public Result<TopNResultValue> apply(Result<TopNResultValue> input)
                              {
                                return new Result<>(
                                    input.getTimestamp(),
                                    new TopNResultValue(
                                        Lists.newArrayList(
                                            Iterables.limit(
                                                input.getValue(),
                                                query.getThreshold()
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

              return new Result<>(
                  input.getTimestamp(),
                  new TopNResultValue(
                      Lists.newArrayList(
                          Iterables.limit(
                              input.getValue(),
                              query.getThreshold()
                          )
                      )
                  )
              );
            }
          }
      );
    }
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> finalQueryDecoration(final QueryRunner<Result<TopNResultValue>> runner)
  {
    return new QueryRunner<Result<TopNResultValue>>()
    {
      @Override
      public Sequence<Result<TopNResultValue>> run(
          Query<Result<TopNResultValue>> query, Map<String, Object> responseContext
      )
      {
        final List<String> outputColumns = ((TopNQuery) query).getOutputColumns();
        final Sequence<Result<TopNResultValue>> result = runner.run(query, responseContext);
        if (outputColumns != null) {
          return Sequences.map(
              result, new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
              {
                @Override
                public Result<TopNResultValue> apply(Result<TopNResultValue> input)
                {
                  DateTime timestamp = input.getTimestamp();
                  List<Map<String, Object>> values = input.getValue().getValue();
                  List<Map<String, Object>> processed = Lists.newArrayListWithExpectedSize(values.size());
                  for (Map<String, Object> holder : values) {
                    Map<String, Object> retained = Maps.newHashMapWithExpectedSize(outputColumns.size());
                    for (String retain : outputColumns) {
                      retained.put(retain, holder.get(retain));
                    }
                    processed.add(retained);
                  }
                  return new Result<>(timestamp, new TopNResultValue(processed));
                }
              }
          );
        } else {
          return result;
        }
      }
    };
  }

  @Override
  public Function<Sequence<Result<TopNResultValue>>, Sequence<Map<String, Object>>> asMap(
      final TopNQuery query, final String timestampColumn
  )
  {
    return new Function<Sequence<Result<TopNResultValue>>, Sequence<Map<String, Object>>>()
    {
      @Override
      public Sequence<Map<String, Object>> apply(Sequence<Result<TopNResultValue>> sequence)
      {
        return Sequences.explode(
            sequence, new Function<Result<TopNResultValue>, Sequence<Map<String, Object>>>()
            {
              @Override
              public Sequence<Map<String, Object>> apply(Result<TopNResultValue> input)
              {
                return Sequences.simple(input.getValue().getValue());
              }
            }
        );
      }
    };
  }

  @Override
  public <I> QueryRunner<Result<TopNResultValue>> handleSubQuery(QuerySegmentWalker segmentWalker, QueryConfig config)
  {
    return new ThresholdAdjustingQueryRunner(
        new SubQueryRunner<I>(segmentWalker, config)
        {
          @Override
          protected Function<Interval, Sequence<Result<TopNResultValue>>> query(
              final Query<Result<TopNResultValue>> query,
              final Segment segment
          )
          {
            final TopNQuery topNQuery = (TopNQuery) query;
            return new Function<Interval, Sequence<Result<TopNResultValue>>>()
            {
              @Override
              public Sequence<Result<TopNResultValue>> apply(Interval interval)
              {
                return engine.query(
                    topNQuery.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)),
                    segment
                );
              }
            };
          }
        }, config.getTopN()
    );
  }
}
