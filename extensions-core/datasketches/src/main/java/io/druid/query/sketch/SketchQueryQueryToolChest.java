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

package io.druid.query.sketch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.granularity.QueryGranularities;
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
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class SketchQueryQueryToolChest extends QueryToolChest<Result<Map<String, Object>>, SketchQuery>
{
  private static final TypeReference<Result<Map<String, Object>>> TYPE_REFERENCE =
      new TypeReference<Result<Map<String, Object>>>()
      {
      };

  private static final TypeReference<Object[]> CACHED_TYPE_REFERENCE = new TypeReference<Object[]>()
  {
  };

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public SketchQueryQueryToolChest(
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Result<Map<String, Object>>> mergeResults(
      QueryRunner<Result<Map<String, Object>>> runner
  )
  {
    return new ResultMergeQueryRunner<Result<Map<String, Object>>>(runner)
    {
      @Override
      protected Ordering<Result<Map<String, Object>>> makeOrdering(Query<Result<Map<String, Object>>> query)
      {
        return ResultGranularTimestampComparator.create(
            QueryGranularities.ALL,
            query.isDescending()
        );
      }

      @Override
      protected BinaryFn<Result<Map<String, Object>>, Result<Map<String, Object>>, Result<Map<String, Object>>>
      createMergeFn(Query<Result<Map<String, Object>>> input)
      {
        final SketchQuery sketch = (SketchQuery) input;
        return new SketchBinaryFn(sketch.getSketchParam(), sketch.getSketchOp().handler());
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SketchQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<Result<Map<String, Object>>, Result<Map<String, Object>>> makePreComputeManipulatorFn(
      final SketchQuery query, MetricManipulationFn fn
  )
  {
    // fn is for aggregators.. we don't need to apply it
    return new Function<Result<Map<String, Object>>, Result<Map<String, Object>>>()
    {
      @Override
      public Result<Map<String, Object>> apply(Result<Map<String, Object>> input)
      {
        Map<String, Object> sketches = input.getValue();
        for (Map.Entry<String, Object> entry : sketches.entrySet()) {
          entry.setValue(TypedSketch.deserialize(query.getSketchOp(), entry.getValue()));
        }
        return input;
      }
    };
  }

  @Override
  public Function<Result<Map<String, Object>>, Result<Map<String, Object>>> makePostComputeManipulatorFn(
      SketchQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<Map<String, Object>>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CacheStrategy<Result<Map<String, Object>>, Object[], SketchQuery> getCacheStrategy(final SketchQuery query)
  {
    return new CacheStrategy<Result<Map<String, Object>>, Object[], SketchQuery>()
    {
      @Override
      public byte[] computeCacheKey(SketchQuery query)
      {
        final byte[] filterBytes = QueryCacheHelper.computeCacheBytes(query.getDimFilter());
        final byte[] vcBytes = QueryCacheHelper.computeAggregatorBytes(query.getVirtualColumns());
        final List<DimensionSpec> dimensions = query.getDimensions();
        final List<String> metrics = query.getMetrics();

        final byte[][] columnsBytes = new byte[dimensions.size() + metrics.size()][];
        int columnsBytesSize = 0;
        int index = 0;
        for (DimensionSpec dimension : dimensions) {
          columnsBytes[index] = dimension.getCacheKey();
          columnsBytesSize += columnsBytes[index].length;
          ++index;
        }
        for (String metric : metrics) {
          columnsBytes[index] = QueryCacheHelper.computeCacheBytes(metric);
          columnsBytesSize += columnsBytes[index].length;
          ++index;
        }

        final ByteBuffer queryCacheKey = ByteBuffer
            .allocate(6 + filterBytes.length + vcBytes.length + columnsBytesSize)
            .put(SKETCH_QUERY)
            .put((byte) query.getSketchOp().ordinal())
            .putInt(query.getSketchParam())
            .put(filterBytes)
            .put(vcBytes);

        for (byte[] bytes : columnsBytes) {
          queryCacheKey.put(bytes);
        }

        return queryCacheKey.array();
      }

      @Override
      public TypeReference<Object[]> getCacheObjectClazz()
      {
        return CACHED_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<Map<String, Object>>, Object[]> prepareForCache()
      {
        final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
        final List<String> metrics = query.getMetrics();
        return new Function<Result<Map<String, Object>>, Object[]>()
        {
          @Override
          public Object[] apply(Result<Map<String, Object>> input)
          {
            final Object[] output = new Object[1 + dimensions.size() + metrics.size()];
            final Map<String, Object> value = input.getValue();

            int index = 0;
            output[index++] = input.getTimestamp().getMillis();
            for (String dimension : dimensions) {
              output[index++] = value.get(dimension);
            }
            for (String metric : metrics) {
              output[index++] = value.get(metric);
            }
            return output;
          }
        };
      }

      @Override
      public Function<Object[], Result<Map<String, Object>>> pullFromCache()
      {
        final SketchOp sketchOp = query.getSketchOp();
        final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
        final List<String> metrics = query.getMetrics();
        return new Function<Object[], Result<Map<String, Object>>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Result<Map<String, Object>> apply(final Object[] input)
          {
            final Map<String, Object> row = Maps.newHashMapWithExpectedSize(dimensions.size() + metrics.size());
            int index = 1;

            for (String dimension : dimensions) {
              row.put(dimension, TypedSketch.deserialize(sketchOp, input[index++]));
            }
            for (String metric : metrics) {
              row.put(metric, TypedSketch.deserialize(sketchOp, input[index++]));
            }
            return new Result<>(new DateTime(((Number) input[0]).longValue()), row);
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<Map<String, Object>>> preMergeQueryDecoration(
      QueryRunner<Result<Map<String, Object>>> runner
  )
  {
    return intervalChunkingQueryRunnerDecorator.decorate(runner, this);
  }

  @Override
  public <I> QueryRunner<Result<Map<String, Object>>> handleSubQuery(
      final QueryRunner<I> subQueryRunner,
      final QuerySegmentWalker segmentWalker,
      final ExecutorService executor,
      final int maxRowCount
  )
  {
    return new SubQueryRunner<I>(subQueryRunner, segmentWalker, executor, maxRowCount)
    {
      @Override
      protected Function<Interval, Sequence<Result<Map<String, Object>>>> function(
          final Query<Result<Map<String, Object>>> query,
          final Map<String, Object> context,
          final Segment segment
      )
      {
        final SketchQuery sketchQuery = (SketchQuery) query;
        final SketchQueryRunner runner = new SketchQueryRunner(segment, null);
        return new Function<Interval, Sequence<Result<Map<String, Object>>>>()
        {
          @Override
          public Sequence<Result<Map<String, Object>>> apply(Interval interval)
          {
            return runner.run(
                sketchQuery.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)), context
            );
          }
        };
      }
    };
  }
}
