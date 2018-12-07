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

package io.druid.query.groupby.orderby;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.orderby.WindowingSpec.PartitionEvaluator;
import io.druid.query.ordering.Accessor;
import io.druid.query.ordering.Comparators;
import io.druid.query.ordering.Direction;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
public class WindowingProcessor implements Function<List<Row>, List<Row>>
{
  private final List<PartitionDefinition> partitions = Lists.newArrayList();

  public WindowingProcessor(
      Query.AggregationsSupport<?> query,
      List<WindowingSpec> windowingSpecs
  )
  {
    Map<String, ValueDesc> expectedTypes = AggregatorFactory.createTypeMap(
        query.getVirtualColumns(), query.getDimensions(), query.getAggregatorSpecs(), query.getPostAggregatorSpecs()
    );

    WindowContext context = WindowContext.newInstance(
        DimensionSpecs.toOutputNames(query.getDimensions()),
        expectedTypes
    );

    for (WindowingSpec windowingSpec : windowingSpecs) {
      List<String> partitionColumns = windowingSpec.getPartitionColumns();
      List<OrderByColumnSpec> orderingSpecs = windowingSpec.getPartitionOrdering();
      Ordering<Row> ordering = makeRowComparator(query, orderingSpecs, false);
      PartitionEvaluator evaluators = windowingSpec.toEvaluator(context.on(partitionColumns, orderingSpecs));
      partitions.add(new PartitionDefinition(partitionColumns, orderingSpecs, ordering, evaluators));
    }
  }

  public List<OrderByColumnSpec> resultOrdering()
  {
    for (int i = partitions.size() - 1; i >= 0; i--) {
      if (partitions.get(i).ordering != null) {
        return partitions.get(i).orderingSpecs;
      }
    }
    return null;
  }

  @Override
  public List<Row> apply(List<Row> input)
  {
    if (input.size() > 0) {
      for (PartitionDefinition partition : partitions) {
        input = partition.process(input);
      }
    }
    return input;
  }

  private static class PartitionDefinition
  {
    private final String[] partColumns;
    private final List<OrderByColumnSpec> orderingSpecs;
    private final Ordering<Row> ordering;
    private final PartitionEvaluator evaluator;

    private final Object[] currPartKeys;
    private Object[] prevPartKeys;

    public PartitionDefinition(
        List<String> partColumns,
        List<OrderByColumnSpec> orderingSpecs,
        Ordering<Row> ordering,
        PartitionEvaluator evaluator
    )
    {
      this.partColumns = partColumns.toArray(new String[partColumns.size()]);
      this.orderingSpecs = orderingSpecs;
      this.ordering = ordering;
      this.evaluator = evaluator;
      this.currPartKeys = new Object[partColumns.size()];
      if (partColumns.isEmpty()) {
        prevPartKeys = new Object[0];
      }
    }

    private List<Row> process(final List<Row> input)
    {
      if (ordering != null) {
        Collections.sort(input, ordering);
      }

      int prev = 0;
      Map<Object[], int[]> partitions = Maps.newLinkedHashMap();
      if (partColumns.length > 0) {
        for (int index = 0; index < input.size(); index++) {
          Row row = input.get(index);
          for (int i = 0; i < partColumns.length; i++) {
            currPartKeys[i] = row.getRaw(partColumns[i]);
          }
          if (prevPartKeys == null) {
            prevPartKeys = Arrays.copyOf(currPartKeys, partColumns.length);
          } else if (!Arrays.equals(prevPartKeys, currPartKeys)) {
            partitions.put(prevPartKeys, new int[]{prev, index});
            prevPartKeys = Arrays.copyOf(currPartKeys, partColumns.length);
            prev = index;
          }
        }
      }
      partitions.put(prevPartKeys, new int[]{prev, input.size()});

      List<Row> rewritten = null;
      for (Map.Entry<Object[], int[]> entry : partitions.entrySet()) {
        int[] index = entry.getValue();
        List<Row> partition = input.subList(index[0], index[1]);
        List<Row> result = evaluator.evaluate(entry.getKey(), partition);
        if (rewritten != null || result != partition) {
          if (rewritten == null) {
            rewritten = index[0] > 0 ? Lists.newArrayList(input.subList(0, index[0] - 1)) : Lists.<Row>newArrayList();
          }
          rewritten.addAll(result);
        }
      }
      return evaluator.finalize(rewritten != null ? rewritten : input);
    }
  }

  public static Accessor<Row> rowAccessor(final String column)
  {
    if (Row.TIME_COLUMN_NAME.equals(column)) {
      return new Accessor<Row>()
      {
        @Override
        public Object get(Row source)
        {
          return source.getTimestampFromEpoch();
        }
      };
    }
    return new Accessor<Row>()
    {
      @Override
      public Object get(Row source)
      {
        return source.getRaw(column);
      }
    };
  }

  public static Accessor<Object[]> arrayAccessor(final List<String> columns, final String column)
  {
    return arrayAccessor(columns.indexOf(column));
  }

  public static Accessor<Object[]> arrayAccessor(final int index)
  {
    return new Accessor<Object[]>()
    {
      @Override
      public Object get(Object[] source)
      {
        return index < 0 ? null : source[index];
      }
    };
  }

  public static Ordering<Row> makeRowComparator(
      Query.AggregationsSupport<?> query,
      List<OrderByColumnSpec> orderingSpecs,
      boolean prependTimeOrdering
  )
  {
    List<Accessor<Row>> accessors = Lists.newArrayList();
    if (prependTimeOrdering) {
      accessors.add(rowAccessor(Row.TIME_COLUMN_NAME));
    }
    for (OrderByColumnSpec ordering : orderingSpecs) {
      accessors.add(rowAccessor(ordering.getDimension()));
    }
    return makeComparator(query, orderingSpecs, accessors, prependTimeOrdering);
  }

  public static Ordering<Object[]> makeArrayComparator(
      Query.AggregationsSupport<?> query,
      List<OrderByColumnSpec> orderingSpecs,
      boolean prependTimeOrdering
  )
  {
    List<String> columns = Lists.newArrayList();
    columns.add(Row.TIME_COLUMN_NAME);
    columns.addAll(DimensionSpecs.toOutputNames(query.getDimensions()));
    columns.addAll(AggregatorFactory.toNames(query.getAggregatorSpecs()));
    columns.addAll(PostAggregators.toNames(query.getPostAggregatorSpecs()));

    List<Accessor<Object[]>> accessors = Lists.newArrayList();
    if (prependTimeOrdering) {
      accessors.add(arrayAccessor(columns, Row.TIME_COLUMN_NAME));
    }
    for (OrderByColumnSpec ordering : orderingSpecs) {
      accessors.add(arrayAccessor(columns, ordering.getDimension()));
    }
    return makeComparator(query, orderingSpecs, accessors, prependTimeOrdering);
  }

  private static <T> Ordering<T> makeComparator(
      Query.AggregationsSupport<?> query,
      List<OrderByColumnSpec> orderingSpecs,
      List<Accessor<T>> accessors,
      boolean prependTimeOrdering
  )
  {
    int index = 0;
    List<Comparator<T>> comparators = Lists.newArrayList();
    if (prependTimeOrdering) {
      final Accessor<T> accessor = accessors.get(index++);
      comparators.add(new Comparator<T>()
      {
        @Override
        public int compare(T left, T right)
        {
          return Longs.compare((Long) accessor.get(left), (Long) accessor.get(right));
        }
      });
    }
    Map<String, DimensionSpec> dimensionsMap = Maps.newHashMap();
    for (DimensionSpec spec : query.getDimensions()) {
      dimensionsMap.put(spec.getOutputName(), spec);
    }

    Map<String, AggregatorFactory> aggregatorsMap = Maps.newHashMap();
    for (AggregatorFactory agg : query.getAggregatorSpecs()) {
      aggregatorsMap.put(agg.getName(), agg);
    }

    Map<String, PostAggregator> postAggregatorsMap = Maps.newHashMap();
    for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
      postAggregatorsMap.put(postAgg.getName(), postAgg);
    }

    for (OrderByColumnSpec columnSpec : orderingSpecs) {
      boolean descending = columnSpec.getDirection() == Direction.DESCENDING;
      String columnName = columnSpec.getDimension();
      Accessor<T> accessor = accessors.get(index++);
      Ordering<T> nextOrdering;
      if (postAggregatorsMap.containsKey(columnName)) {
        nextOrdering = metricOrdering(accessor, postAggregatorsMap.get(columnName).getComparator(), descending);
      } else if (aggregatorsMap.containsKey(columnName)) {
        nextOrdering = metricOrdering(accessor, aggregatorsMap.get(columnName).getComparator(), descending);
      } else if (dimensionsMap.containsKey(columnName)) {
        nextOrdering = dimensionOrdering(accessor, columnSpec.getComparator());
      } else {
        nextOrdering = metricOrdering(accessor, descending);  // last resort.. assume it's assigned by expression
      }

      comparators.add(nextOrdering);
    }

    return Comparators.compound(comparators);
  }

  private static <T> Ordering<T> metricOrdering(final Accessor<T> accessor, Comparator comparator, boolean descending)
  {
    final Ordering ordering = Ordering.from(comparator).nullsFirst();
    Ordering<T> metric = new Ordering<T>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public int compare(T left, T right)
      {
        return ordering.compare(accessor.get(left), accessor.get(right));
      }
    };
    return descending ? metric.reverse() : metric;
  }

  private static <T> Ordering<T> metricOrdering(final Accessor<T> accessor, boolean descending)
  {
    final Ordering ordering = Ordering.natural().nullsFirst();
    Ordering<T> metric = new Ordering<T>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public int compare(T left, T right)
      {
        Comparable l = (Comparable) accessor.get(left);
        Comparable r = (Comparable) accessor.get(right);
        return ordering.compare(l, r);
      }
    };
    return descending ? metric.reverse() : metric;
  }

  @SuppressWarnings("unchecked")
  private static <T> Ordering<T> dimensionOrdering(final Accessor<T> accessor, final Comparator comparator)
  {
    return Ordering
        .from(comparator)
        .onResultOf(
            new Function<T, Comparable>()
            {
              @Override
              public Comparable apply(T input)
              {
                // Multi-value dimensions have all been flattened at this point;
                return (Comparable) accessor.get(input);
              }
            }
        );
  }
}

