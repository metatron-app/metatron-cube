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

package io.druid.query.groupby.orderby;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.query.RowResolver;
import io.druid.query.groupby.orderby.WindowingSpec.PartitionEvaluator;
import io.druid.query.select.Schema;
import io.druid.query.select.StreamQuery;
import io.druid.segment.serde.ComplexMetrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
public class WindowingProcessor implements Function<List<Row>, List<Row>>
{
  private final OrderingProcessor processor;
  private final List<WindowingSpec> windowingSpecs;
  private final WindowContext context;

  public WindowingProcessor(OrderingProcessor processor, Schema schema, List<WindowingSpec> windowingSpecs)
  {
    this.processor = processor;
    this.windowingSpecs = windowingSpecs;
    this.context = WindowContext.newInstance(schema.getColumnNames(), GuavaUtils.asMap(schema.columnAndTypes()));
  }

  public WindowingProcessor(StreamQuery query, RowResolver resolver, List<WindowingSpec> windowingSpecs)
  {
    Map<String, ValueDesc> typeMap = Maps.newHashMap();
    Map<String, Comparator> comparatorMap = Maps.newHashMap();
    for (String column : query.getColumns()) {
      ValueDesc type = resolver.resolve(column, ValueDesc.UNKNOWN);
      if (type.isUnknown()) {
        continue;
      }
      Comparator comparator = ComplexMetrics.getComparator(type);
      if (comparator != null) {
        comparatorMap.put(column, comparator);
      }
      typeMap.put(column, type);
    }
    this.processor = new OrderingProcessor(query.getColumns(), comparatorMap);
    this.windowingSpecs = windowingSpecs;
    this.context = WindowContext.newInstance(query.getColumns(), typeMap);
  }

  public OrderingProcessor ordering()
  {
    Map<String, Comparator> comparatorMap = Maps.newHashMap();
    for (String assigned : context.getOutputColumns()) {
      comparatorMap.put(assigned, ComplexMetrics.getComparator(context.resolve(assigned)));
    }
    return new OrderingProcessor(context.getOutputColumns(), comparatorMap);
  }

  @Override
  public List<Row> apply(List<Row> input)
  {
    for (WindowingSpec windowingSpec : windowingSpecs) {
      input = createPartitionSpec(windowingSpec).process(input);
    }
    return input;
  }

  private PartitionDefinition createPartitionSpec(WindowingSpec windowingSpec)
  {
    List<String> partitionColumns = windowingSpec.getPartitionColumns();
    List<OrderByColumnSpec> orderingSpecs = windowingSpec.getRequiredOrdering();
    PartitionEvaluator evaluators = windowingSpec.toEvaluator(context.on(partitionColumns, orderingSpecs));

    Ordering<Row> ordering = processor.toRowOrdering(orderingSpecs, false);
    return new PartitionDefinition(partitionColumns, orderingSpecs, ordering, evaluators);
  }

  public Ordering<Row> toRowOrdering(List<OrderByColumnSpec> columns)
  {
    return processor.toRowOrdering(rewriteOrdering(columns), false);
  }

  public Ordering<Object[]> toArrayOrdering(List<OrderByColumnSpec> columns)
  {
    return processor.toArrayOrdering(rewriteOrdering(columns), false);
  }

  private List<OrderByColumnSpec> rewriteOrdering(List<OrderByColumnSpec> columns)
  {
    List<OrderByColumnSpec> rewritten = Lists.newArrayList();
    for (OrderByColumnSpec order : columns) {
      ValueDesc type = context.resolve(order.getDimension(), ValueDesc.UNKNOWN);
      if (!type.isUnknown() && !type.isStringOrDimension()) {
        order = order.withComparator(type.typeName());  // todo add types to context for flatten spec
      }
      rewritten.add(order);
    }
    return rewritten;
  }

  public List<String> getFinalColumns()
  {
    return context.getOutputColumns();
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
      this.partColumns = partColumns.toArray(new String[0]);
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
        final int[] index = entry.getValue();
        final List<Row> partition = input.subList(index[0], index[1]);
        if (partition.isEmpty()) {
          continue;
        }
        final List<Row> result = evaluator.evaluate(entry.getKey(), partition);
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
}

