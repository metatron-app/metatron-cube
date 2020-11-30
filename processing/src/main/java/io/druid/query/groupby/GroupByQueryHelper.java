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

import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.BaseQuery;
import io.druid.query.QueryConfig;
import io.druid.query.groupby.orderby.OrderedLimitSpec;
import io.druid.segment.incremental.IncrementalIndex;

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
    if (BaseQuery.isBySegment(query)) {
      return new DummyMergeIndex(query);
    }
    final int maxResults = config.getMaxResults(query);
    final OrderedLimitSpec nodeLimit = query.getLimitSpec().getNodeLimit();
    if (config.useParallelSort(query) ||
        nodeLimit != null && nodeLimit.hasLimit() && nodeLimit.getLimit() < maxResults) {
      return new MergeIndexParallel(query.withPostAggregatorSpecs(null), maxResults, parallelism);
    }
    return new MergeIndexSorting(query.withPostAggregatorSpecs(null), maxResults, parallelism);
  }

  @SuppressWarnings("unchecked")
  private static class DummyMergeIndex implements MergeIndex
  {
    private final GroupByQuery groupBy;
    private final Queue queue = new ConcurrentLinkedQueue();

    private DummyMergeIndex(GroupByQuery groupBy)
    {
      this.groupBy = groupBy;
    }

    @Override
    public void add(Object row)
    {
      queue.add(row);
    }

    @Override
    public Sequence toMergeStream(boolean compact)
    {
      return Sequences.simple(groupBy.estimatedInitialColumns(), queue);
    }

    @Override
    public void close()
    {
      queue.clear();
    }
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

  private static final int DEFAULT_POLLING_INTERVAL = 10000;

  public static <T> Accumulator<MergeIndex<T>, T> newMergeAccumulator(final Execs.Semaphore semaphore)
  {
    return new Accumulator<MergeIndex<T>, T>()
    {
      private int counter;

      @Override
      public MergeIndex<T> accumulate(final MergeIndex<T> accumulated, final T in)
      {
        if (++counter % DEFAULT_POLLING_INTERVAL == 0 && semaphore.isDestroyed()) {
          return MergeIndex.NULL;
        }
        accumulated.add(in);
        return accumulated;
      }
    };
  }
}
