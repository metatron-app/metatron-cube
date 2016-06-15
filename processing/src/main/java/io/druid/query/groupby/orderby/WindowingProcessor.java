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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.orderby.WindowingSpec.PartitionEvaluator;
import io.druid.query.ordering.StringComparators;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
public class WindowingProcessor implements Function<List<Row>, List<Row>>
{
  private Map<Ordering<Row>, List<PartitionDefinition>> partitionMaps;

  public WindowingProcessor(
      List<WindowingSpec> windowingSpecs,
      List<DimensionSpec> dimensionSpecs,
      List<AggregatorFactory> factories,
      List<PostAggregator> postAggregators
  )
  {
    Map<List<OrderByColumnSpec>, List<WindowingSpec>> windowingMaps =
        Maps.<List<OrderByColumnSpec>, List<WindowingSpec>>newLinkedHashMap();
    for (WindowingSpec windowingSpec : windowingSpecs) {
      List<OrderByColumnSpec> partColumns = toPartitionKey(windowingSpec);
      List<WindowingSpec> sameSpecs = windowingMaps.get(partColumns);
      if (sameSpecs == null) {
        windowingMaps.put(ImmutableList.copyOf(partColumns), sameSpecs = Lists.newArrayList());
      }
      sameSpecs.add(windowingSpec);
    }
    partitionMaps = Maps.newLinkedHashMap();
    for (Map.Entry<List<OrderByColumnSpec>, List<WindowingSpec>> entry : windowingMaps.entrySet()) {
      List<OrderByColumnSpec> orderingSpecs = entry.getKey();
      Ordering<Row> ordering = makeComparator(orderingSpecs, dimensionSpecs, factories, postAggregators);
      List<PartitionDefinition> partitions = Lists.newArrayList();
      for (WindowingSpec spec : entry.getValue()) {
        String[] partColumns = spec.getPartitionColumns().toArray(new String[0]);
        PartitionEvaluator evaluators = spec.toEvaluator(factories, postAggregators);
        partitions.add(new PartitionDefinition(partColumns, evaluators));
      }
      partitionMaps.put(ordering, partitions);
    }
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
    for (Map.Entry<Ordering<Row>, List<PartitionDefinition>> entry : partitionMaps.entrySet()) {
      Collections.sort(input, entry.getKey());
      for (PartitionDefinition partition : entry.getValue()) {
        partition.process(input);
      }
    }
    return input;
  }

  private static class PartitionDefinition
  {
    private final String[] partColumns;
    private final PartitionEvaluator evaluator;

    private final Object[] currPartKeys;
    private Object[] prevPartKeys;

    public PartitionDefinition(String[] partColumns, PartitionEvaluator evaluator)
    {
      this.partColumns = partColumns;
      this.evaluator = evaluator;
      this.currPartKeys = new Object[partColumns.length];
    }

    private void process(final List<Row> input)
    {
      int prev = 0;
      Map<Object[], int[]> partitions = Maps.newLinkedHashMap();
      if (partColumns.length > 0) {
        for (int index = 0; index < input.size(); index++) {
          MapBasedRow row = (MapBasedRow) input.get(index);
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

      for (Map.Entry<Object[], int[]> entry : partitions.entrySet()) {
        int[] partition = entry.getValue();
        evaluator.evaluate(entry.getKey(), input.subList(partition[0], partition[1]));
      }
    }
  }

  public static Ordering<Row> makeComparator(
      List<OrderByColumnSpec> orderingSpecs,
      List<DimensionSpec> dimensions, List<AggregatorFactory> aggs, List<PostAggregator> postAggs
  )
  {
    Ordering<Row> ordering = new Ordering<Row>()
    {
      @Override
      public int compare(Row left, Row right)
      {
        return Longs.compare(left.getTimestampFromEpoch(), right.getTimestampFromEpoch());
      }
    };

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
      Ordering<Row> nextOrdering = null;

      if (postAggregatorsMap.containsKey(columnName)) {
        nextOrdering = metricOrdering(columnName, postAggregatorsMap.get(columnName).getComparator());
      } else if (aggregatorsMap.containsKey(columnName)) {
        nextOrdering = metricOrdering(columnName, aggregatorsMap.get(columnName).getComparator());
      } else if (dimensionsMap.containsKey(columnName)) {
        nextOrdering = dimensionOrdering(columnName, columnSpec.getDimensionComparator());
      }

      if (nextOrdering == null) {
        throw new ISE("Unknown column in order clause[%s]", columnSpec);
      }

      switch (columnSpec.getDirection()) {
        case DESCENDING:
          nextOrdering = nextOrdering.reverse();
      }

      ordering = ordering.compound(nextOrdering);
    }

    return ordering;
  }

  private static Ordering<Row> metricOrdering(final String column, final Comparator comparator)
  {
    return new Ordering<Row>()
    {
      @SuppressWarnings("unchecked")
      @Override
      public int compare(Row left, Row right)
      {
        return comparator.compare(left.getRaw(column), right.getRaw(column));
      }
    };
  }

  private static Ordering<Row> dimensionOrdering(
      final String dimension,
      final StringComparators.StringComparator comparator
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

