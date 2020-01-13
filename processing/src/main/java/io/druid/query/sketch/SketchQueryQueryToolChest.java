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

package io.druid.query.sketch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.druid.common.DateTimes;
import io.druid.common.KeyBuilder;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.CacheStrategy;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class SketchQueryQueryToolChest extends QueryToolChest.CacheSupport<Result<Object[]>, Object[], SketchQuery>
{
  private static final TypeReference<Result<Object[]>> TYPE_REFERENCE =
      new TypeReference<Result<Object[]>>()
      {
      };

  private static final TypeReference<Object[]> CACHED_TYPE_REFERENCE = new TypeReference<Object[]>()
  {
  };

  private final GenericQueryMetricsFactory queryMetricsFactory;
  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public SketchQueryQueryToolChest(
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator,
      GenericQueryMetricsFactory queryMetricsFactory
  )
  {
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<Result<Object[]>> mergeResults(
      QueryRunner<Result<Object[]>> runner
  )
  {
    return new ResultMergeQueryRunner<Result<Object[]>>(runner)
    {
      @Override
      protected Ordering<Result<Object[]>> makeOrdering(Query<Result<Object[]>> query)
      {
        return ResultGranularTimestampComparator.create(QueryGranularities.ALL, query.isDescending());
      }

      @Override
      protected BinaryFn<Result<Object[]>, Result<Object[]>, Result<Object[]>>
      createMergeFn(Query<Result<Object[]>> input)
      {
        final SketchQuery sketch = (SketchQuery) input;
        return new SketchBinaryFn(sketch.getSketchParamWithDefault(), sketch.getSketchOp().handler());
      }
    };
  }

  @Override
  public QueryMetrics<? super SketchQuery> makeMetrics(SketchQuery query)
  {
    return queryMetricsFactory.makeMetrics(query);
  }

  @Override
  public Function<Result<Object[]>, Result<Object[]>> makePreComputeManipulatorFn(
      final SketchQuery query, MetricManipulationFn fn
  )
  {
    // fn is for aggregators.. we don't need to apply it
    return new Function<Result<Object[]>, Result<Object[]>>()
    {
      @Override
      public Result<Object[]> apply(Result<Object[]> input)
      {
        // todo currently, sketch query supports natural ordering only
        Object[] sketches = input.getValue();
        for (int i = 0; i < sketches.length; i++) {
          sketches[i] = TypedSketch.deserialize(query.getSketchOp(), sketches[i], null);
        }
        return input;
      }
    };
  }

  @Override
  public TypeReference<Result<Object[]>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CacheStrategy<Result<Object[]>, Object[], SketchQuery> getCacheStrategy(final SketchQuery query)
  {
    return new CacheStrategy<Result<Object[]>, Object[], SketchQuery>()
    {
      @Override
      public byte[] computeCacheKey(SketchQuery query)
      {
        return KeyBuilder.get()
                         .append(SKETCH_QUERY)
                         .appendByte(query.getSketchOp())
                         .append(query.getSketchParam())
                         .append(query.getFilter())
                         .append(query.getVirtualColumns())
                         .append(query.getDimensions())
                         .append(query.getMetrics())
                         .build();
      }

      @Override
      public TypeReference<Object[]> getCacheObjectClazz()
      {
        return CACHED_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<Object[]>, Object[]> prepareForCache()
      {
        final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
        final List<String> metrics = query.getMetrics();
        return new Function<Result<Object[]>, Object[]>()
        {
          @Override
          public Object[] apply(Result<Object[]> input)
          {
            final Object[] output = new Object[1 + dimensions.size() + metrics.size()];
            final Object[] value = input.getValue();

            int index = 0;
            output[index] = input.getTimestamp().getMillis();
            for (String dimension : dimensions) {
              output[index + 1] = value[index++];
            }
            for (String metric : metrics) {
              output[index + 1] = value[index++];
            }
            output[0] = input.getTimestamp().getMillis();
            return output;
          }
        };
      }

      @Override
      public Function<Object[], Result<Object[]>> pullFromCache()
      {
        final SketchOp sketchOp = query.getSketchOp();
        final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
        final List<String> metrics = query.getMetrics();
        return new Function<Object[], Result<Object[]>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Result<Object[]> apply(final Object[] input)
          {
            final Object[] row = new Object[dimensions.size() + metrics.size()];
            int index = 0;
            final long timestamp = ((Number) input[index++]).longValue();
            for (String dimension : dimensions) {
              row[index - 1] = TypedSketch.deserialize(sketchOp, input[index++], null);
            }
            for (String metric : metrics) {
              row[index - 1] = TypedSketch.deserialize(sketchOp, input[index++], null);
            }
            return new Result<>(DateTimes.utc(timestamp), row);
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<Object[]>> preMergeQueryDecoration(
      QueryRunner<Result<Object[]>> runner
  )
  {
    return intervalChunkingQueryRunnerDecorator.decorate(runner, this);
  }

  @Override
  public <I> QueryRunner<Result<Object[]>> handleSubQuery(QuerySegmentWalker segmentWalker, QueryConfig config)
  {
    return new SubQueryRunner<I>(segmentWalker, config)
    {
      @Override
      protected Function<Interval, Sequence<Result<Object[]>>> query(
          final Query<Result<Object[]>> query,
          final Segment segment
      )
      {
        final SketchQuery sketchQuery = (SketchQuery) query;
        final SketchQueryRunner runner = new SketchQueryRunner(segment, null);
        return new Function<Interval, Sequence<Result<Object[]>>>()
        {
          @Override
          public Sequence<Result<Object[]>> apply(Interval interval)
          {
            return runner.run(
                sketchQuery.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)), null
            );
          }
        };
      }
    };
  }
}
