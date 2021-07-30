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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.query.RowSignature;
import io.druid.query.groupby.orderby.WindowingSpec.PartitionEvaluator;
import io.druid.query.ordering.Accessor;
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

  public WindowingProcessor(OrderingProcessor processor, RowSignature schema, List<WindowingSpec> windowingSpecs)
  {
    this.processor = processor;
    this.windowingSpecs = windowingSpecs;
    this.context = WindowContext.newInstance(schema.getColumnNames(), GuavaUtils.asMap(schema.columnAndTypes()));
  }

  public WindowingProcessor(StreamQuery query, TypeResolver resolver, List<WindowingSpec> windowingSpecs)
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

    List<Accessor<Row>> accessors = OrderingProcessor.rowAccessors(partitionColumns);
    Comparator<Row> ordering = processor.toRowOrdering(orderingSpecs, OrderingProcessor.rowAccessors(
        Iterables.transform(orderingSpecs, OrderByColumnSpec::getDimension)
    ));
    PartitionEvaluator evaluators = windowingSpec.toEvaluator(context.on(partitionColumns, orderingSpecs));
    return new PartitionDefinition(accessors, ordering, windowingSpec.getInputLimit(), evaluators);
  }

  public Comparator<Row> toRowOrdering(List<OrderByColumnSpec> columns)
  {
    List<OrderByColumnSpec> orderingSpecs = rewriteOrdering(columns);
    return processor.toRowOrdering(
        orderingSpecs,
        OrderingProcessor.rowAccessors(Iterables.transform(orderingSpecs, OrderByColumnSpec::getDimension))
    );
  }

  public Comparator<Object[]> toArrayOrdering(List<OrderByColumnSpec> columns)
  {
    return processor.toArrayOrdering(rewriteOrdering(columns), false);
  }

  private List<OrderByColumnSpec> rewriteOrdering(List<OrderByColumnSpec> columns)
  {
    List<OrderByColumnSpec> rewritten = Lists.newArrayList();
    for (OrderByColumnSpec order : columns) {
      ValueDesc type = context.resolve(order.getDimension(), ValueDesc.UNKNOWN).unwrapDimension();
      if (!type.isUnknown() && !type.isString()) {
        order = order.withComparator(type.typeName());  // todo add types to context for flatten spec
      }
      rewritten.add(order);
    }
    return rewritten;
  }

  // should be called after processed
  public List<String> getFinalColumns(Map<String, String> alias)
  {
    if (alias.isEmpty()) {
      return context.getOutputColumns();
    }
    List<String> aliased = Lists.newArrayList();
    for (String name : context.getOutputColumns()) {
      aliased.add(alias.getOrDefault(name, name));
    }
    return aliased;
  }

  private static class PartitionDefinition
  {
    private final List<Accessor<Row>> partColumns;
    private final Comparator<Row> ordering;
    private final int inputLimit;
    private final PartitionEvaluator evaluator;

    private final Object[] currPartKeys;
    private Object[] prevPartKeys;

    public PartitionDefinition(
        List<Accessor<Row>> partColumns,
        Comparator<Row> ordering,
        int inputLimit,
        PartitionEvaluator evaluator
    )
    {
      this.partColumns = partColumns;
      this.ordering = ordering;
      this.inputLimit = inputLimit;
      this.evaluator = evaluator;
      this.currPartKeys = new Object[partColumns.size()];
      if (partColumns.isEmpty()) {
        prevPartKeys = new Object[0];
      }
    }

    private List<Row> process(List<Row> rows)
    {
      final List<Row> input = prepareInputRows(rows);

      int prev = 0;
      Map<Object[], int[]> partitions = Maps.newLinkedHashMap();
      if (!partColumns.isEmpty()) {
        for (int index = 0; index < input.size(); index++) {
          Row row = input.get(index);
          for (int i = 0; i < partColumns.size(); i++) {
            currPartKeys[i] = partColumns.get(i).get(row);
          }
          if (prevPartKeys == null) {
            prevPartKeys = Arrays.copyOf(currPartKeys, currPartKeys.length);
          } else if (!Arrays.equals(prevPartKeys, currPartKeys)) {
            partitions.put(prevPartKeys, new int[]{prev, index});
            prevPartKeys = Arrays.copyOf(currPartKeys, currPartKeys.length);
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

    private List<Row> prepareInputRows(List<Row> input)
    {
      if (inputLimit > 0 && inputLimit < input.size()) {
        if (ordering == null) {
          input = input.subList(0, inputLimit);
        } else {
          input = Lists.newArrayList(TopNSorter.topN(ordering, input, inputLimit));
        }
      } else if (ordering != null) {
        Collections.sort(input, ordering);
      }
      return input;
    }
  }
}

