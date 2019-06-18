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

package io.druid.query.groupby;

import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import io.druid.collections.StupidPool;
import io.druid.concurrent.Execs;
import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.QueryConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.orderby.OrderedLimitSpec;
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
  public static MergeIndex createMergeIndex(
      final GroupByQuery query,
      final QueryConfig config,
      final int parallelism
  )
  {
    final int maxResults = config.getMaxResults(query);
    final OrderedLimitSpec nodeLimit = query.getLimitSpec().getNodeLimit();
    if (config.useParallelSort(query) ||
        nodeLimit != null && nodeLimit.hasLimit() && nodeLimit.getLimit() < maxResults) {
      return new MergeIndexParallel(query.withPostAggregatorSpecs(null), maxResults, parallelism);
    }
    return new MergeIndexSorting(query.withPostAggregatorSpecs(null), maxResults, parallelism);
  }

  public static IncrementalIndex createIncrementalIndex(
      final GroupByQuery query,
      final StupidPool<ByteBuffer> bufferPool,
      final boolean sortFacts,
      final int maxRowCount,
      final List<ValueType> groupByTypes
  )
  {
    final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
    final List<AggregatorFactory> aggs = AggregatorFactory.toCombinerFactory(query.getAggregatorSpecs());

    // use granularity truncated min timestamp since incoming truncated timestamps may precede timeStart
    IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(Long.MIN_VALUE)
        .withQueryGranularity(Granularities.ALL)
        .withMetrics(aggs.toArray(new AggregatorFactory[0]))
        .withFixedSchema(true)
        .withRollup(true)
        .build();

    final IncrementalIndex index;
    if (query.getContextValue("useOffheap", false)) {
      index = new OffheapIncrementalIndex(schema, false, true, sortFacts, false, maxRowCount, bufferPool);
    } else {
      index = new OnheapIncrementalIndex(schema, false, true, sortFacts, false, maxRowCount);
    }
    index.initialize(dimensions, groupByTypes);
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

  private static final int DEFAULT_POLLING_INTERVAL = 100000;

  public static <T> Accumulator<MergeIndex, T> newMergeAccumulator(final Execs.Semaphore semaphore)
  {
    return new Accumulator<MergeIndex, T>()
    {
      private int counter;

      @Override
      public MergeIndex accumulate(final MergeIndex accumulated, final T in)
      {
        if (++counter % DEFAULT_POLLING_INTERVAL == 0 && semaphore.isDestroyed()) {
          return accumulated;
        }
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
