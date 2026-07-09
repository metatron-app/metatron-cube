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
  // Opt-in (system property, gated on !rollup below): when the input rows are already sorted by time, skip the
  // sorted TreeMap and just APPEND into an insertion-order LinkedHashMap. rollup=false already gives every row a
  // unique key (NoRollup indexer) and TimeAndDims uses identity hashCode/equals, so the map never merges — it is
  // a pure O(1) append, and persist consumes it in insertion (= time) order. Removes the (dims-comparing) TreeMap
  // insert that dominates the build. Only safe for a persist-only, pre-time-sorted, rollup=false build.
  public static final boolean APPEND_PROP = "true".equalsIgnoreCase(System.getProperty("druid.incrementalIndex.presortedAppend"));

  private final boolean append;
  private final NavigableMap<TimeAndDims, Object[]> facts;       // sorted mode
  private final Map<TimeAndDims, Object[]> appendFacts;          // append mode (insertion-order)
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

    this.append = APPEND_PROP && !indexSchema.isRollup();
    if (append) {
      this.facts = null;
      this.appendFacts = new java.util.LinkedHashMap<>();
    } else if (indexSchema.isNoQuery()) {
      this.facts = new TreeMap<>(dimsComparator());
      this.appendFacts = null;
    } else {
      this.facts = new ConcurrentSkipListMap<>(dimsComparator());
      this.appendFacts = null;
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
  public Map<TimeAndDims, Object[]> getRangeOf(long from, long to, Boolean timeDescending)
  {
    if (append) {
      // insertion order == time order (input is pre-sorted). persist asks for the full range; a sub-range
      // (queries — not used on this build path) is a linear, order-preserving filter.
      if (from <= io.druid.java.util.common.JodaUtils.MIN_INSTANT
          && to >= io.druid.java.util.common.JodaUtils.MAX_INSTANT
          && (timeDescending == null || !timeDescending)) {
        return appendFacts;   // full range (persist path)
      }
      final Map<TimeAndDims, Object[]> out = new java.util.LinkedHashMap<>();
      for (final Map.Entry<TimeAndDims, Object[]> e : appendFacts.entrySet()) {
        if (e.getKey().timestamp >= from && e.getKey().timestamp < to) {
          out.put(e.getKey(), e.getValue());
        }
      }
      return out;
    }
    return getFacts(facts, from, to, timeDescending);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected final void addToFacts(TimeAndDims key) throws IndexSizeExceededException
  {
    final long _t0 = System.nanoTime();
    final Object[] current = (append ? appendFacts : facts).computeIfAbsent(key, populator);
    final long _t1 = System.nanoTime();
    Prof.factsPutNanos.addAndGet(_t1 - _t0);   // the sorted TreeMap insert (the "sort")
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
    Prof.aggNanos.addAndGet(System.nanoTime() - _t1);   // aggregators (count + raw relay store)
  }

  /** Profiling: split the add() cost into the sorted facts-put vs the aggregators. dict-encode = add - these. */
  public static final class Prof
  {
    public static final java.util.concurrent.atomic.AtomicLong factsPutNanos = new java.util.concurrent.atomic.AtomicLong();
    public static final java.util.concurrent.atomic.AtomicLong aggNanos = new java.util.concurrent.atomic.AtomicLong();

    private Prof() {}
  }

  @Override
  @SuppressWarnings("unchecked")
  public long estimatedOccupation()
  {
    long estimation = super.estimatedOccupation();
    if (estimableIndices.length > 0) {
      for (final Object[] array : (append ? appendFacts : facts).values()) {
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
    (append ? appendFacts : facts).clear();
  }
}
