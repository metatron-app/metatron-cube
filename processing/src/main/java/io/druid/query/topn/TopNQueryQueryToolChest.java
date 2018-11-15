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

package io.druid.query.topn;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValue;
import io.druid.query.CacheStrategy;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.TabularFormat;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class TopNQueryQueryToolChest extends QueryToolChest<Result<TopNResultValue>, TopNQuery>
{
  private static final TypeReference<Result<TopNResultValue>> TYPE_REFERENCE = new TypeReference<Result<TopNResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };

  private final Supplier<TopNQueryConfig> config;
  private final TopNQueryEngine engine;
  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public TopNQueryQueryToolChest(
      Supplier<TopNQueryConfig> config,
      TopNQueryEngine engine,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.config = config;
    this.engine = engine;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  protected static String[] extractFactoryName(final List<AggregatorFactory> aggregatorFactories)
  {
    return Lists.transform(
        aggregatorFactories, new Function<AggregatorFactory, String>()
        {
          @Override
          public String apply(AggregatorFactory input)
          {
            return input.getName();
          }
        }
    ).toArray(new String[0]);
  }

  private static List<PostAggregator> prunePostAggregators(TopNQuery query)
  {
    return AggregatorUtil.pruneDependentPostAgg(
        query.getPostAggregatorSpecs(),
        query.getTopNMetricSpec().getMetricName(query.getDimensionSpec())
    );
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> mergeResults(
      QueryRunner<Result<TopNResultValue>> runner
  )
  {
    return new ResultMergeQueryRunner<Result<TopNResultValue>>(runner)
    {
      @Override
      protected Ordering<Result<TopNResultValue>> makeOrdering(Query<Result<TopNResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(query.getGranularity(), query.isDescending());
      }

      @Override
      protected BinaryFn<Result<TopNResultValue>, Result<TopNResultValue>, Result<TopNResultValue>> createMergeFn(
          Query<Result<TopNResultValue>> input
      )
      {
        TopNQuery query = (TopNQuery) input;
        return new TopNBinaryFn(
            TopNResultMerger.identity,
            query.getGranularity(),
            query.getDimensionSpec(),
            query.getTopNMetricSpec(),
            query.getThreshold(),
            query.getAggregatorSpecs(),
            query.getPostAggregatorSpecs()
        );
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(TopNQuery query)
  {
    final List<AggregatorFactory> aggregators = query.getAggregatorSpecs();
    return super.makeMetricBuilder(query)
                .setDimension("threshold", String.valueOf(query.getThreshold()))
                .setDimension("dimension", query.getDimensionSpec().getDimension())
                .setDimension("numMetrics", String.valueOf(aggregators.size()))
                .setDimension("numComplexMetrics", String.valueOf(DruidMetrics.findNumComplexAggs(aggregators)));
  }

  @Override
  public Function<Result<TopNResultValue>, Result<TopNResultValue>> makePreComputeManipulatorFn(
      final TopNQuery query, final MetricManipulationFn fn
  )
  {
    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      private String dimension = query.getDimensionSpec().getOutputName();
      private final List<PostAggregator> prunedAggs = PostAggregators.decorate(
          prunePostAggregators(query),
          query.getAggregatorSpecs()
      );
      private final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs()
                                                                   .toArray(new AggregatorFactory[0]);
      private final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());

      @Override
      public Result<TopNResultValue> apply(final Result<TopNResultValue> result)
      {
        final DateTime timestamp = result.getTimestamp();
        List<Map<String, Object>> serializedValues = Lists.newArrayList(
            Iterables.transform(
                result.getValue(),
                new Function<Map<String, Object>, Map<String, Object>>()
                {
                  @Override
                  public Map<String, Object> apply(Map<String, Object> input)
                  {
                    final Map<String, Object> values = Maps.newHashMapWithExpectedSize(
                        aggregatorFactories.length
                        + prunedAggs.size()
                        + 1
                    );

                    for (int i = 0; i < aggregatorFactories.length; ++i) {
                      final String aggName = aggFactoryNames[i];
                      values.put(aggName, fn.manipulate(aggregatorFactories[i], input.get(aggName)));
                    }

                    for (PostAggregator postAgg : prunedAggs) {
                      final String name = postAgg.getName();
                      Object calculatedPostAgg = input.get(name);
                      if (calculatedPostAgg != null) {
                        values.put(name, calculatedPostAgg);
                      } else {
                        values.put(name, postAgg.compute(timestamp, values));
                      }
                    }
                    values.put(dimension, input.get(dimension));

                    return values;
                  }
                }
            )
        );

        return new Result<TopNResultValue>(
            result.getTimestamp(),
            new TopNResultValue(serializedValues)
        );
      }
    };
  }

  @Override
  public Function<Result<TopNResultValue>, Result<TopNResultValue>> makePostComputeManipulatorFn(
      final TopNQuery query, final MetricManipulationFn fn
  )
  {
    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      private String dimension = query.getDimensionSpec().getOutputName();
      private final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs()
                                                                   .toArray(new AggregatorFactory[0]);
      private final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());
      private final PostAggregator[] postAggregators = PostAggregators.decorate(
          query.getPostAggregatorSpecs(), aggregatorFactories
      ).toArray(new PostAggregator[0]);

      @Override
      public Result<TopNResultValue> apply(Result<TopNResultValue> result)
      {
        final DateTime timestamp = result.getTimestamp();
        List<Map<String, Object>> serializedValues = Lists.newArrayList(
            Iterables.transform(
                result.getValue(),
                new Function<Map<String, Object>, Map<String, Object>>()
                {
                  @Override
                  public Map<String, Object> apply(Map<String, Object> input)
                  {
                    final Map<String, Object> values = Maps.newHashMapWithExpectedSize(
                        aggregatorFactories.length
                        + query.getPostAggregatorSpecs().size()
                        + 1
                    );
                    values.put(dimension, input.get(dimension));

                    for (final String name : aggFactoryNames) {
                      values.put(name, input.get(name));
                    }

                    for (PostAggregator postAgg : postAggregators) {
                      Object calculatedPostAgg = input.get(postAgg.getName());
                      if (calculatedPostAgg != null) {
                        values.put(postAgg.getName(), calculatedPostAgg);
                      } else {
                        values.put(postAgg.getName(), postAgg.compute(timestamp, values));
                      }
                    }
                    for (int i = 0; i < aggFactoryNames.length; ++i) {
                      final String name = aggFactoryNames[i];
                      values.put(name, fn.manipulate(aggregatorFactories[i], input.get(name)));
                    }

                    return values;
                  }
                }
            )
        );

        return new Result<>(
            result.getTimestamp(),
            new TopNResultValue(serializedValues)
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
  public CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> getCacheStrategy(final TopNQuery query)
  {
    return new CacheStrategy<Result<TopNResultValue>, Object, TopNQuery>()
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
        final byte[] vcBytes = QueryCacheHelper.computeCacheKeys(query.getVirtualColumns());
        final byte[] dimensionSpecBytes = query.getDimensionSpec().getCacheKey();
        final byte[] metricSpecBytes = query.getTopNMetricSpec().getCacheKey();

        final DimFilter dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] aggregatorBytes = QueryCacheHelper.computeCacheKeys(query.getAggregatorSpecs());
        final byte[] granularityBytes = query.getGranularity().getCacheKey();
        final byte[] outputColumnsBytes = QueryCacheHelper.computeCacheBytes(query.getOutputColumns());

        return ByteBuffer
            .allocate(
                1 + vcBytes.length + dimensionSpecBytes.length + metricSpecBytes.length + 4 +
                granularityBytes.length + filterBytes.length + aggregatorBytes.length + outputColumnsBytes.length
            )
            .put(TOPN_QUERY)
            .put(vcBytes)
            .put(dimensionSpecBytes)
            .put(metricSpecBytes)
            .put(Ints.toByteArray(query.getThreshold()))
            .put(granularityBytes)
            .put(filterBytes)
            .put(aggregatorBytes)
            .put(outputColumnsBytes)
            .array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TopNResultValue>, Object> prepareForCache()
      {
        return new Function<Result<TopNResultValue>, Object>()
        {
          private final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());

          @Override
          public Object apply(final Result<TopNResultValue> input)
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
      public Function<Object, Result<TopNResultValue>> pullFromCache()
      {
        return new Function<Object, Result<TopNResultValue>>()
        {
          private final Granularity granularity = query.getGranularity();

          @Override
          @SuppressWarnings("unchecked")
          public Result<TopNResultValue> apply(Object input)
          {
            List<Object> results = (List<Object>) input;
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
            if (topNQuery.getDimensionsFilter() != null) {
              topNQuery = topNQuery.withDimFilter(topNQuery.getDimensionsFilter().optimize());
            }
            final TopNQuery delegateTopNQuery = topNQuery;
            if (TopNQueryEngine.canApplyExtractionInPost(delegateTopNQuery)) {
              final DimensionSpec dimensionSpec = delegateTopNQuery.getDimensionSpec();
              return runner.run(
                  delegateTopNQuery.withDimensionSpec(
                      new DefaultDimensionSpec(
                          dimensionSpec.getDimension(),
                          dimensionSpec.getOutputName()
                      )
                  ), responseContext
              );
            } else {
              return runner.run(delegateTopNQuery, responseContext);
            }
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
          return Sequences.map(
              resultSequence, new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
              {
                @Override
                public Result<TopNResultValue> apply(Result<TopNResultValue> input)
                {
                  TopNResultValue resultValue = input.getValue();

                  return new Result<TopNResultValue>(
                      input.getTimestamp(),
                      new TopNResultValue(
                          Lists.transform(
                              resultValue.getValue(),
                              new Function<Map<String, Object>, Map<String, Object>>()
                              {
                                @Override
                                public Map<String, Object> apply(Map<String, Object> input)
                                {
                                  String dimOutputName = topNQuery.getDimensionSpec().getOutputName();
                                  Object dimValue = input.get(dimOutputName);
                                  input.put(
                                      dimOutputName,
                                      topNQuery.getDimensionSpec().getExtractionFn().apply(dimValue)
                                  );
                                  return input;
                                }
                              }
                          )
                      )
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
    private final Supplier<TopNQueryConfig> config;

    public ThresholdAdjustingQueryRunner(
        QueryRunner<Result<TopNResultValue>> runner,
        Supplier<TopNQueryConfig> config
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
      final int minTopNThreshold = query.getContextValue("minTopNThreshold", config.get().getMinTopNThreshold());
      if (query.getThreshold() > minTopNThreshold) {
        return runner.run(query, responseContext);
      }

      final boolean isBySegment = BaseQuery.getContextBySegment(query, false);

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
        final List<String> outputColumns = ((TopNQuery)query).getOutputColumns();
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
  public TabularFormat toTabularFormat(
      final TopNQuery query, final Sequence<Result<TopNResultValue>> sequence, final String timestampColumn
  )
  {
    return new TabularFormat()
    {
      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return Sequences.concat(
            Sequences.map(
                sequence, new Function<Result<TopNResultValue>, Sequence<Map<String, Object>>>()
                {
                  @Override
                  public Sequence<Map<String, Object>> apply(Result<TopNResultValue> input)
                  {
                    return Sequences.simple(input.getValue().getValue());
                  }
                }
            )
        );
      }

      @Override
      public Map<String, Object> getMetaData()
      {
        return null;
      }
    };
  }

  @Override
  public <I> QueryRunner<Result<TopNResultValue>> handleSubQuery(
      final QueryRunner<I> subQueryRunner,
      final QuerySegmentWalker segmentWalker,
      final ExecutorService executor,
      final int maxRowCount
  )
  {
    return new ThresholdAdjustingQueryRunner(
        new SubQueryRunner<I>(subQueryRunner, segmentWalker, executor, maxRowCount)
        {
          @Override
          protected Function<Interval, Sequence<Result<TopNResultValue>>> function(
              final Query<Result<TopNResultValue>> query, Map<String, Object> context,
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
        }, config
    );
  }
}
