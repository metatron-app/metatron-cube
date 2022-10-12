/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.AggregatorFactory;

import java.io.Closeable;
import java.util.Arrays;
import java.util.function.Consumer;

public interface MergeIndex<T> extends Closeable
{
  void add(T row);

  Sequence<T> toMergeStream(boolean parallel, boolean compact);

  default void close() {}

  MergeIndex NULL = new MergeIndex()
  {
    @Override
    public void add(Object row) {}

    @Override
    public Sequence toMergeStream(boolean parallel, boolean compact) { return Sequences.empty();}
  };

  abstract class GroupByMerge implements MergeIndex<Row>
  {
    protected final Consumer<Object[]> consumer;
    protected final GroupByQuery groupBy;

    protected final BinaryFn.Identical[] metrics;
    protected final int metricStart;

    GroupByMerge(GroupByQuery groupBy)
    {
    this.groupBy = groupBy;
    this.metrics = AggregatorFactory.toCombiner(groupBy.getAggregatorSpecs());
    this.metricStart = groupBy.getDimensions().size() + 1;
      final int[][] groupings = groupBy.getGroupings();
      if (groupings.length == 0) {
        consumer = values -> _addRow(values);
      } else {
        final int metricStart = groupBy.getDimensions().size() + 1;
        consumer = values -> {
          for (final int[] grouping : groupings) {
            final Object[] copy = Arrays.copyOf(values, values.length);
            Arrays.fill(copy, 1, metricStart, null);
            for (int index : grouping) {
              copy[index + 1] = values[index + 1];
            }
            _addRow(copy);
          }
        };
      }
    }

    @Override
    public final void add(Row row)
    {
      consumer.accept(((CompactRow) row).getValues());
    }

    protected abstract void _addRow(Object[] values);
  }
}
