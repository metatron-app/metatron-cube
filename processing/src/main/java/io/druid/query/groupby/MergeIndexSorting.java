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

import com.google.common.collect.Iterables;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.common.guava.Comparators;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 */
public final class MergeIndexSorting extends MergeIndex.GroupByMerge
{
  private static final Logger LOG = new Logger(MergeIndexSorting.class);

  private final GroupByQuery groupBy;

  private final AggregatorFactory.Combiner[] metrics;
  private final int metricStart;

  private final Map<Object[], Object[]> mapping;
  private final BiFunction<Object[], Object[], Object[]> populator;

  public MergeIndexSorting(
      final GroupByQuery groupBy,
      final int maxRowCount,
      final int parallelism
  )
  {
    super(groupBy);
    this.groupBy = groupBy;
    this.metrics = AggregatorFactory.toCombinerArray(groupBy.getAggregatorSpecs());
    this.metricStart = groupBy.getDimensions().size() + 1;

    final Comparator<Object[]> comparator =
        Comparators.toArrayComparator(
            DimensionSpecs.toComparator(groupBy.getDimensions(), true),
            Granularities.ALL.equals(groupBy.getGranularity()) ? 1 : 0
        );
    this.mapping = parallelism == 1 ?
                   new TreeMap<Object[], Object[]>(comparator) :
                   new ConcurrentSkipListMap<Object[], Object[]>(comparator);

    this.populator = new BiFunction<Object[], Object[], Object[]>()
    {
      private final AtomicInteger counter = new AtomicInteger();    // not-exact. size() is very heavy in sorted map

      @Override
      @SuppressWarnings("unchecked")
      public Object[] apply(final Object[] key, final Object[] prevValue)
      {
        if (prevValue == null) {
          if (counter.incrementAndGet() >= maxRowCount) {
            throw new ISE("Maximum number of rows [%d] reached", maxRowCount);
          }
          return key;
        }
        for (int i = 0; i < metrics.length; i++) {
          final int index = metricStart + i;
          prevValue[index] = metrics[i].combine(prevValue[index], key[index]);
        }
        return prevValue;
      }
    };
  }

  @Override
  protected void _addRow(Object[] values)
  {
    mapping.compute(values, populator);
  }

  @Override
  public Sequence<Row> toMergeStream(final boolean compact)
  {
    return Sequences.simple(
        Iterables.transform(mapping.values(), GroupByQueryEngine.arrayToRow(groupBy, compact))
    );
  }

  @Override
  public void close()
  {
    mapping.clear();
  }
}
