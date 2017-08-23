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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OffheapIncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

public class GroupByQueryHelper
{
  public static final String CTX_KEY_MAX_RESULTS = "maxResults";
  public static final String CTX_KEY_FUDGE_TIMESTAMP = "fudgeTimestamp";

  public static IncrementalIndex createMergeIndex(
      final GroupByQuery query,
      final StupidPool<ByteBuffer> bufferPool,
      final boolean sortFacts,
      final int maxResult,
      final Future<Object> optimizer
  )
  {
    final List<DimensionSpec> dimensions = query.getDimensions();
    final List<AggregatorFactory> aggs = Lists.transform(
        query.getAggregatorSpecs(),
        new Function<AggregatorFactory, AggregatorFactory>()
        {
          @Override
          public AggregatorFactory apply(AggregatorFactory input)
          {
            return input.getCombiningFactory();
          }
        }
    );

    // use granularity truncated min timestamp since incoming truncated timestamps may precede timeStart
    IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(Long.MIN_VALUE)
        .withQueryGranularity(QueryGranularities.ALL)
        .withMetrics(aggs.toArray(new AggregatorFactory[aggs.size()]))
        .withFixedSchema(true)
        .withRollup(true)
        .build();

    int maxRowCount = Math.min(
        query.getContextValue(CTX_KEY_MAX_RESULTS, maxResult),
        maxResult
    );

    final IncrementalIndex index;
    if (query.getContextValue("useOffheap", false)) {
      index = new OffheapIncrementalIndex(schema, false, true, sortFacts, maxRowCount, bufferPool);
    } else {
      index = new OnheapIncrementalIndex(schema, false, true, sortFacts, maxRowCount);
    }

    if (optimizer != null) {
      index.initialize((Map<String, String[]>) Futures.getUnchecked(optimizer));
    } else if (dimensions != null) {
      index.initialize(DimensionSpecs.toOutputNames(dimensions));
    }
    return index;
  }

  public static <T> Accumulator<IncrementalIndex, T> newIndexAccumulator()
  {
    return new Accumulator<IncrementalIndex, T>()
    {
      @Override
      public IncrementalIndex accumulate(final IncrementalIndex accumulated, final T in)
      {
        accumulated.add((Row) in);
        return accumulated;
      }
    };
  }

  public static <T> Pair<Queue, Accumulator<Queue, T>> createBySegmentAccumulatorPair()
  {
    // In parallel query runner multiple threads add to this queue concurrently
    Queue init = new ConcurrentLinkedQueue<>();
    Accumulator<Queue, T> accumulator = new Accumulator<Queue, T>()
    {
      @Override
      public Queue accumulate(Queue accumulated, T in)
      {
        if (in == null) {
          throw new ISE("Cannot have null result");
        }
        accumulated.offer(in);
        return accumulated;
      }
    };
    return new Pair<>(init, accumulator);
  }
}
