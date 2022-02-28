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

package io.druid.segment.incremental;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 */
public class OnheapIncrementalIndex extends IncrementalIndex
{
  private final NavigableMap<TimeAndDims, Object[]> facts;
  private final Function<TimeAndDims, Object[]> populator;

  private final int[] estimableIndices;
  private final AtomicInteger counter = new AtomicInteger();

  public OnheapIncrementalIndex(
      final IncrementalIndexSchema indexSchema,
      final boolean deserializeComplexMetrics,
      final boolean reportParseExceptions,
      final boolean estimate,
      final int maxRowCount
  )
  {
    super(indexSchema, deserializeComplexMetrics, reportParseExceptions, estimate, maxRowCount);

    if (indexSchema.isNoQuery()) {
      this.facts = new TreeMap<>(dimsComparator());
    } else {
      this.facts = new ConcurrentSkipListMap<>(dimsComparator());
    }
    this.populator = new Function<TimeAndDims, Object[]>()
    {
      @Override
      public Object[] apply(TimeAndDims timeAndDims)
      {
        if (counter.incrementAndGet() > maxRowCount) {
          counter.decrementAndGet();
          throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
        }
        return new Object[aggregators.length];
      }
    };
    final List<Integer> estimableIndices = Lists.newArrayList();
    for (int i = 0; i < aggregators.length; i++) {
      if (aggregators[i] instanceof Aggregator.Estimable) {
        estimableIndices.add(i);
      }
    }
    this.estimableIndices = Ints.toArray(estimableIndices);
  }

  @Override
  public boolean canAppendRow()
  {
    return counter.get() < maxRowCount;
  }

  @VisibleForTesting
  public OnheapIncrementalIndex(
      long minTimestamp,
      Granularity gran,
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean rollup,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .withRollup(rollup)
                                            .build(),
        deserializeComplexMetrics,
        reportParseExceptions,
        false,
        maxRowCount
    );
  }

  @VisibleForTesting
  public OnheapIncrementalIndex(long minTimestamp, Granularity gran, AggregatorFactory[] metrics, int maxRowCount)
  {
    this(minTimestamp, gran, true, metrics, maxRowCount);
  }

  @VisibleForTesting
  public OnheapIncrementalIndex(
      long minTimestamp,
      Granularity gran,
      boolean rollup,
      AggregatorFactory[] metrics,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .withRollup(rollup)
                                            .build(),
        true,
        true,
        true,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean reportParseExceptions,
      int maxRowCount
  )
  {
    this(incrementalIndexSchema, true, reportParseExceptions, true, maxRowCount);
  }

  @Override
  public Iterable<Map.Entry<TimeAndDims, Object[]>> getRangeOf(long from, long to, Boolean timeDescending)
  {
    return getFacts(facts, from, to, timeDescending);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected final void addToFacts(TimeAndDims key) throws IndexSizeExceededException
  {
    final Object[] current = facts.computeIfAbsent(key, populator);
    for (int i = 0; i < aggregators.length; i++) {
      try {
        current[i] = aggregators[i].aggregate(current[i]);
      }
      catch (ParseException e) {
        // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
        if (reportParseExceptions) {
          throw new ParseException(e, "Encountered parse error for aggregator[%s]", aggregators[i]);
        }
        LOG.debug(e, "Encountered parse error, skipping aggregator[%s].", aggregators[i]);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public long estimatedOccupation()
  {
    long estimation = super.estimatedOccupation();
    if (estimableIndices.length > 0) {
      for (final Object[] array : facts.values()) {
        for (int index : estimableIndices) {
          estimation += ((Aggregator.Estimable) aggregators[index]).estimateOccupation(array[index]);
        }
      }
    }
    return estimation;
  }

  @Override
  public int size()
  {
    return counter.get();   // ConcurrentSkipListMap.size() is very very expensive (10x than indexing itself)
  }

  /**
   * Clear out maps to allow GC
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();
    facts.clear();
  }
}
