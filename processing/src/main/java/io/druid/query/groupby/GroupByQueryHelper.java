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

import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OffheapIncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GroupByQueryHelper
{
  public static final String CTX_KEY_MAX_RESULTS = "maxResults";
  public static final String CTX_KEY_FUDGE_TIMESTAMP = "fudgeTimestamp";

  public static MergeIndex createMergeIndex(
      final GroupByQuery query,
      final StupidPool<ByteBuffer> bufferPool,
      final int maxResult,
      final int parallelism,
      final boolean compact
  )
  {
    int maxRowCount = Math.min(query.getContextValue(CTX_KEY_MAX_RESULTS, maxResult), maxResult);
    if (query.getContextBoolean(Query.GBY_MERGE_SIMPLE, true)) {
      return new SimpleMergeIndex(query.getDimensions(), query.getAggregatorSpecs(), maxRowCount, parallelism, compact);
    } else {
      return createIncrementalIndex(query, bufferPool, false, maxRowCount);
    }
  }

  public static IncrementalIndex createIncrementalIndex(
      final GroupByQuery query,
      final StupidPool<ByteBuffer> bufferPool,
      final boolean sortFacts,
      final int maxRowCount
  )
  {
    final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
    final List<AggregatorFactory> aggs = AggregatorFactory.toCombiner(query.getAggregatorSpecs());

    // use granularity truncated min timestamp since incoming truncated timestamps may precede timeStart
    IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(Long.MIN_VALUE)
        .withQueryGranularity(QueryGranularities.ALL)
        .withMetrics(aggs.toArray(new AggregatorFactory[aggs.size()]))
        .withFixedSchema(true)
        .withRollup(true)
        .build();

    final IncrementalIndex index;
    if (query.getContextValue("useOffheap", false)) {
      index = new OffheapIncrementalIndex(schema, false, true, sortFacts, maxRowCount, bufferPool);
    } else {
      index = new OnheapIncrementalIndex(schema, false, true, sortFacts, maxRowCount);
    }
    index.initialize(dimensions);
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

  public static <T> Accumulator<MergeIndex, T> newMergeAccumulator()
  {
    return new Accumulator<MergeIndex, T>()
    {
      @Override
      public MergeIndex accumulate(final MergeIndex accumulated, final T in)
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
