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
import com.google.inject.Inject;
import io.druid.common.KeyBuilder;
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
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.Interval;

/**
 */
public class SketchQueryQueryToolChest extends QueryToolChest.CacheSupport<Object[], Object[], SketchQuery>
{
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
  public QueryRunner<Object[]> mergeResults(QueryRunner<Object[]> runner)
  {
    return new ResultMergeQueryRunner<Object[]>(runner)
    {
      @Override
      protected BinaryFn<Object[], Object[], Object[]> createMergeFn(Query<Object[]> input)
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
  public Function<Object[], Object[]> makePreComputeManipulatorFn(SketchQuery query, MetricManipulationFn fn)
  {
    // fn is for aggregators.. we don't need to apply it
    return new Function<Object[], Object[]>()
    {
      private final SketchOp sketchOp = query.getSketchOp();

      @Override
      public Object[] apply(Object[] input)
      {
        // todo currently, sketch query supports natural ordering only
        for (int i = 1; i < input.length; i++) {
          input[i] = TypedSketch.deserialize(sketchOp, input[i], null);
        }
        return input;
      }
    };
  }

  @Override
  public TypeReference<Object[]> getResultTypeReference(SketchQuery query)
  {
    return ARRAY_TYPE_REFERENCE;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CacheStrategy<Object[], Object[], SketchQuery> getCacheStrategy(final SketchQuery query)
  {
    return new IdenticalCacheStrategy()
    {
      @Override
      public byte[] computeCacheKey(SketchQuery query)
      {
        return KeyBuilder.get()
                         .append(SKETCH_QUERY)
                         .append(query.getSketchOp())
                         .append(query.getSketchParam())
                         .append(query.getFilter())
                         .append(query.getVirtualColumns())
                         .append(query.getDimensions())
                         .append(query.getMetrics())
                         .build();
      }
    };
  }

  @Override
  public QueryRunner<Object[]> preMergeQueryDecoration(QueryRunner<Object[]> runner)
  {
    return intervalChunkingQueryRunnerDecorator.decorate(runner, this);
  }

  @Override
  public <I> QueryRunner<Object[]> handleSubQuery(QuerySegmentWalker segmentWalker, QueryConfig config)
  {
    return new SubQueryRunner<I>(segmentWalker, config)
    {
      @Override
      protected Function<Interval, Sequence<Object[]>> query(final Query<Object[]> query, final Segment segment)
      {
        final SketchQuery sketchQuery = (SketchQuery) query;
        final SketchQueryRunner runner = new SketchQueryRunner(segment, null);
        return new Function<Interval, Sequence<Object[]>>()
        {
          @Override
          public Sequence<Object[]> apply(Interval interval)
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
