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
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.orderby.WindowingSpec.PartitionEvaluator;
import io.druid.query.ordering.StringComparator;

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
      List<WindowingSpec> windowingSpecs,
      List<DimensionSpec> dimensionSpecs,
      List<AggregatorFactory> factories,
      List<PostAggregator> postAggregators
  )
  {
    for (WindowingSpec windowingSpec : windowingSpecs) {
      List<OrderByColumnSpec> orderingSpecs = toPartitionKey(windowingSpec);
      Ordering<Row> ordering = makeComparator(orderingSpecs, dimensionSpecs, factories, postAggregators, false);
      String[] partColumns = windowingSpec.getPartitionColumns().toArray(new String[0]);
      PartitionEvaluator evaluators = windowingSpec.toEvaluator(factories, postAggregators);
      partitions.add(new PartitionDefinition(partColumns, orderingSpecs, ordering, evaluators));
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

  private List<OrderByColumnSpec> toPartitionKey(WindowingSpec spec)
  {
    List<String> partitionColumns = spec.getPartitionColumns();
    List<OrderByColumnSpec> sortingColumns = spec.getSortingColumns();

    if (partitionColumns.isEmpty()) {
      return sortingColumns;
    }
    List<OrderByColumnSpec> merged = Lists.newArrayList();
    List<String> sortingColumnNames = Lists.transform(sortingColumns, OrderByColumnSpec.GET_DIMENSION);
    int i = 0;
    for (; i < partitionColumns.size(); i++) {
      if (sortingColumnNames.indexOf(partitionColumns.get(i)) != i) {
        break;
      }
      merged.add(sortingColumns.get(0));
    }
    for (int j = i; j < partitionColumns.size(); j++) {
      merged.add(new OrderByColumnSpec(partitionColumns.get(j), OrderByColumnSpec.Direction.ASCENDING));
    }
    merged.addAll(sortingColumns.subList(i, sortingColumns.size()));
    return merged;
  }

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
        String[] partColumns,
        List<OrderByColumnSpec> orderingSpecs,
        Ordering<Row> ordering,
        PartitionEvaluator evaluator
    )
    {
      this.partColumns = partColumns;
      this.orderingSpecs = orderingSpecs;
      this.ordering = ordering;
      this.evaluator = evaluator;
      this.currPartKeys = new Object[partColumns.length];
      if (partColumns.length == 0) {
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
      return rewritten != null ? rewritten : input;
    }
  }

  public static Ordering<Row> makeComparator(
      List<OrderByColumnSpec> orderingSpecs,
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggs,
      List<PostAggregator> postAggs,
      boolean prependTimeOrdering
  )
  {
    Ordering<Row> ordering = prependTimeOrdering ? new Ordering<Row>()
    {
      @Override
      public int compare(Row left, Row right)
      {
        return Longs.compare(left.getTimestampFromEpoch(), right.getTimestampFromEpoch());
      }
    } : null;

    Map<String, DimensionSpec> dimensionsMap = Maps.newHashMap();
    for (DimensionSpec spec : dimensions) {
      dimensionsMap.put(spec.getOutputName(), spec);
    }

    Map<String, AggregatorFactory> aggregatorsMap = Maps.newHashMap();
    for (final AggregatorFactory agg : aggs) {
      aggregatorsMap.put(agg.getName(), agg);
    }

    Map<String, PostAggregator> postAggregatorsMap = Maps.newHashMap();
    for (PostAggregator postAgg : postAggs) {
      postAggregatorsMap.put(postAgg.getName(), postAgg);
    }

    for (OrderByColumnSpec columnSpec : orderingSpecs) {
      String columnName = columnSpec.getDimension();
      Ordering<Row> nextOrdering;
      if (postAggregatorsMap.containsKey(columnName)) {
        nextOrdering = metricOrdering(columnName, postAggregatorsMap.get(columnName).getComparator());
      } else if (aggregatorsMap.containsKey(columnName)) {
        nextOrdering = metricOrdering(columnName, aggregatorsMap.get(columnName).getComparator());
      } else if (dimensionsMap.containsKey(columnName)) {
        nextOrdering = dimensionOrdering(columnName, columnSpec.getDimensionComparator());
      } else {
        nextOrdering = metricOrdering(columnName);  // last resort.. assume it's assigned by expression
      }

      switch (columnSpec.getDirection()) {
        case DESCENDING:
          nextOrdering = nextOrdering.reverse();
      }

      ordering = ordering == null ? nextOrdering : ordering.compound(nextOrdering);
    }

    return ordering;
  }

  private static Ordering<Row> metricOrdering(final String column, final Comparator comparator)
  {
    final Ordering ordering = Ordering.from(comparator).nullsFirst();
    return new Ordering<Row>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public int compare(Row left, Row right)
      {
        return ordering.compare(left.getRaw(column), right.getRaw(column));
      }
    };
  }

  private static Ordering<Row> metricOrdering(final String column)
  {
    final Ordering ordering = Ordering.natural().nullsFirst();
    return new Ordering<Row>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public int compare(Row left, Row right)
      {
        Comparable l = (Comparable) left.getRaw(column);
        Comparable r = (Comparable) right.getRaw(column);
        return ordering.compare(l, r);
      }
    };
  }

  private static Ordering<Row> dimensionOrdering(
      final String dimension,
      final StringComparator comparator
  )
  {
    return Ordering
        .from(comparator)
        .nullsFirst()
        .onResultOf(
            new Function<Row, String>()
            {
              @Override
              public String apply(Row input)
              {
                // Multi-value dimensions have all been flattened at this point;
                Object value = input.getRaw(dimension);
                return value == null ? null : String.valueOf(value);
              }
            }
        );
  }
}

