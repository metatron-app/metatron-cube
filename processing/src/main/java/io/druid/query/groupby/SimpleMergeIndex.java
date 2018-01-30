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

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectArray;
import io.druid.segment.incremental.IncrementalIndex;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 */
public class SimpleMergeIndex implements MergeIndex
{
  private final String[] dimensions;
  private final AggregatorFactory[] metrics;

  private final RowSupplier rowSupplier = new RowSupplier();
  private final Map<TimeAndDims, Aggregator[]> mapping = new ConcurrentHashMap<>();
  private final Function<TimeAndDims, Aggregator[]> populator;

  public SimpleMergeIndex(List<DimensionSpec> dimensions, List<AggregatorFactory> aggregators, final int maxRowCount)
  {
    this.dimensions = DimensionSpecs.toOutputNames(dimensions).toArray(new String[0]);
    this.metrics = AggregatorFactory.toCombiner(aggregators).toArray(new AggregatorFactory[0]);

    final ColumnSelectorFactory[] selectors = new ColumnSelectorFactory[aggregators.size()];
    for (int i = 0; i < selectors.length; i++) {
      selectors[i] = new ColumnSelectorFactories.Caching(
          new ColumnSelectorFactories.FromInputRow(rowSupplier, metrics[i], false)
      );
    }
    this.populator = new Function<TimeAndDims, Aggregator[]>()
    {
      @Override
      public Aggregator[] apply(TimeAndDims timeAndDims)
      {
        if (mapping.size() >= maxRowCount) {
          throw new ISE("Maximum number of rows [%d] reached", maxRowCount);
        }
        final Aggregator[] aggregators = new Aggregator[metrics.length];
        for (int i = 0; i < aggregators.length; i++) {
          aggregators[i] = metrics[i].factorize(selectors[i]);
        }
        return aggregators;
      }
    };
  }

  @Override
  public void add(Row row)
  {
    rowSupplier.set(row);
    final String[] key = new String[dimensions.length];
    for (int i = 0; i < key.length; i++) {
      key[i] = Objects.toString(row.getRaw(dimensions[i]), "");
    }
    final TimeAndDims timeAndDims = new TimeAndDims(row.getTimestampFromEpoch(), key);

    for (Aggregator agg : mapping.computeIfAbsent(timeAndDims, populator)) {
      agg.aggregate();
    }
  }

  @Override
  public Iterable<Row> toMergeStream()
  {
    return Lists.transform(
        IncrementalIndex.sortOn(mapping, true),
        new com.google.common.base.Function<Map.Entry<TimeAndDims, Aggregator[]>, Row>()
        {
          @Override
          public Row apply(Map.Entry<TimeAndDims, Aggregator[]> input)
          {
            final TimeAndDims key = input.getKey();
            final Aggregator[] value = input.getValue();
            Map<String, Object> event = Maps.newLinkedHashMap();
            for (int i = 0; i < dimensions.length; i++) {
              event.put(dimensions[i], Strings.emptyToNull(key.get(i)));
            }
            for (int i = 0; i < metrics.length; i++) {
              event.put(metrics[i].getName(), value[i].get());
            }
            return new MapBasedRow(key.timestamp, event);
          }
        }
    );
  }

  @Override
  public void close() throws IOException
  {
    mapping.clear();
    rowSupplier.close();
  }

  private static class RowSupplier implements Supplier<Row>, Closeable
  {
    private final ThreadLocal<Row> in = new ThreadLocal<>();

    public void set(Row row)
    {
      in.set(row);
    }

    @Override
    public Row get()
    {
      return in.get();
    }

    @Override
    public void close()
    {
      in.remove();
    }
  }

  private static class TimeAndDims extends ObjectArray<String> implements Comparable<TimeAndDims>
  {
    private final long timestamp;

    public TimeAndDims(long timestamp, String[] array)
    {
      super(array);
      this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o)
    {
      TimeAndDims other = (TimeAndDims) o;
      return timestamp == other.timestamp && Arrays.equals(array, other.array);
    }

    @Override
    public int hashCode()
    {
      return Arrays.hashCode(array) * 31 + Long.hashCode(timestamp);
    }

    @Override
    public String toString()
    {
      return timestamp + super.toString();
    }

    @Override
    public int compareTo(TimeAndDims o)
    {
      int compare = Longs.compare(timestamp, o.timestamp);
      for (int i = 0; compare == 0 && i < array.length; i++) {
        compare = array[i].compareTo(o.array[i]);
      }
      return compare;
    }
  }
}
