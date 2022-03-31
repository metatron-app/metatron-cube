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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.Cacheable;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.Query;
import io.druid.query.RowSignature;
import io.druid.query.groupby.orderby.WindowContext.Frame;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 */
public class WindowingSpec implements RowSignature.Evolving
{
  public static WindowingSpec expressions(String... expressions)
  {
    return new WindowingSpec(null, null, null, null, false, -1, Arrays.asList(expressions), null, null);
  }

  private final List<String> partitionColumns;
  private final List<OrderByColumnSpec> sortingColumns;
  private final Integer increment;
  private final Integer offset;
  private final boolean skipSorting;
  private final int inputLimit;

  private final List<String> expressions;
  private final FlattenSpec flattenSpec;
  private final PivotSpec pivotSpec;

  @JsonCreator
  public WindowingSpec(
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("sortingColumns") List<OrderByColumnSpec> sortingColumns,
      @JsonProperty("increment") Integer increment,
      @JsonProperty("offset") Integer offset,
      @JsonProperty("skipSorting") boolean skipSorting,
      @JsonProperty("inputLimit") int inputLimit,
      @JsonProperty("expressions") List<String> expressions,
      @JsonProperty("flattenSpec") FlattenSpec flattenSpec,
      @JsonProperty("pivotSpec") PivotSpec pivotSpec
  )
  {
    this.partitionColumns = partitionColumns == null ? ImmutableList.<String>of() : partitionColumns;
    this.sortingColumns = sortingColumns == null ? ImmutableList.<OrderByColumnSpec>of() : sortingColumns;
    this.increment = increment;
    this.offset = offset;
    this.skipSorting = skipSorting;
    this.inputLimit = inputLimit;
    this.expressions = expressions == null ? ImmutableList.<String>of() : expressions;
    this.flattenSpec = flattenSpec;
    this.pivotSpec = pivotSpec;
    Preconditions.checkArgument((flattenSpec == null && pivotSpec == null) || increment == null);
    Preconditions.checkArgument((flattenSpec == null && pivotSpec == null) || offset == null);
  }

  public WindowingSpec(
      List<String> partitionColumns,
      List<OrderByColumnSpec> sortingColumns,
      List<String> expressions,
      FlattenSpec flattenSpec
  )
  {
    this(partitionColumns, sortingColumns, null, null, false, -1, expressions, flattenSpec, null);
  }

  public WindowingSpec(
      List<String> partitionColumns,
      List<OrderByColumnSpec> sortingColumns,
      List<String> expressions,
      PivotSpec pivotSpec
  )
  {
    this(partitionColumns, sortingColumns, null, null, false, -1, expressions, null, pivotSpec);
  }

  public WindowingSpec(List<String> partitionColumns, List<OrderByColumnSpec> sortingColumns, String... expressions)
  {
    this(partitionColumns, sortingColumns, null, null, Arrays.asList(expressions));
  }

  public WindowingSpec(
      List<String> partitionColumns,
      List<OrderByColumnSpec> sortingColumns,
      Integer increment,
      Integer offset,
      List<String> expressions
  )
  {
    this(partitionColumns, sortingColumns, increment, offset, false, -1, expressions, null, null);
  }

  // used by pre-ordering (gby, stream)
  public WindowingSpec skipSorting()
  {
    return new WindowingSpec(
        partitionColumns,
        null,
        increment,
        offset,
        true,
        inputLimit,
        expressions,
        flattenSpec,
        pivotSpec
    );
  }

  public WindowingSpec withInputLimit(int inputLimit)
  {
    return new WindowingSpec(
        partitionColumns,
        sortingColumns,
        increment,
        offset,
        skipSorting,
        inputLimit,
        expressions,
        flattenSpec,
        pivotSpec
    );
  }

  public WindowingSpec withIncrement(int increment)
  {
    return new WindowingSpec(
        partitionColumns,
        sortingColumns,
        increment,
        offset,
        skipSorting,
        inputLimit,
        expressions,
        flattenSpec,
        pivotSpec
    );
  }

  private static List<OrderByColumnSpec> toOrderingSpec(
      List<String> partitionColumns,
      List<OrderByColumnSpec> sortingColumns
  )
  {
    if (GuavaUtils.isNullOrEmpty(partitionColumns)) {
      return sortingColumns == null ? ImmutableList.<OrderByColumnSpec>of() : sortingColumns;
    }
    if (GuavaUtils.isNullOrEmpty(sortingColumns)) {
      return OrderByColumnSpec.ascending(partitionColumns);
    }
    List<OrderByColumnSpec> merged = Lists.newArrayList();
    List<String> sortingColumnNames = OrderByColumnSpec.getColumns(sortingColumns);
    for (String partitionColumn : partitionColumns) {
      final int index = sortingColumnNames.indexOf(partitionColumn);
      if (index < 0) {
        merged.add(OrderByColumnSpec.asc(partitionColumn));
      } else {
        sortingColumnNames.set(index, null);
        merged.add(sortingColumns.get(index));
      }
    }
    for (int i = 0; i < sortingColumnNames.size(); i++) {
      if (sortingColumnNames.get(i) != null) {
        merged.add(sortingColumns.get(i));
      }
    }
    return merged;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getPartitionColumns()
  {
    return partitionColumns;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<OrderByColumnSpec> getSortingColumns()
  {
    return sortingColumns;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public Integer getIncrement()
  {
    return increment;
  }

  @JsonProperty
  public boolean isSkipSorting()
  {
    return skipSorting;
  }

  @JsonProperty
  public int getInputLimit()
  {
    return inputLimit;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getExpressions()
  {
    return expressions;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public FlattenSpec getFlattenSpec()
  {
    return flattenSpec;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public PivotSpec getPivotSpec()
  {
    return pivotSpec;
  }

  @JsonIgnore
  public List<OrderByColumnSpec> getRequiredOrdering()
  {
    return skipSorting ? ImmutableList.<OrderByColumnSpec>of() : toOrderingSpec(partitionColumns, sortingColumns);
  }

  public PartitionEvaluator toEvaluator(final WindowContext context)
  {
    PartitionEvaluator evaluator = expressions.isEmpty() ? new PartitionEvaluator() : new PartitionEvaluator()
    {
      @Override
      public List<Row> evaluate(Object[] partitionKey, final List<Row> partition)
      {
        int increment = Optional.ofNullable(WindowingSpec.this.increment).orElse(1);
        int offset = Optional.ofNullable(WindowingSpec.this.offset).orElse(increment - 1);
        return context.with(partitionKey, partition)
                      .evaluate(
                          Iterables.transform(expressions, expr -> Frame.of(Evals.splitAssign(expr, context))),
                          increment,
                          offset
                      );
      }
    };
    if (flattenSpec != null) {
      evaluator = chain(evaluator, flattenSpec.create(context));
    }
    if (pivotSpec != null) {
      evaluator = chain(evaluator, pivotSpec.create(context));
    }
    return evaluator;
  }

  static Expr.NumericBinding withMap(final Map<String, ?> bindings)
  {
    return new Expr.NumericBinding()
    {
      private final Map<String, Integer> cache = Maps.newHashMap();

      @Override
      public Collection<String> names()
      {
        return bindings.keySet();
      }

      @Override
      public Object get(String name)
      {
        // takes target[column.value] or target[index], use '_' instead of '-' for negative index (backward)
        Object value = bindings.get(name);
        if (value != null || bindings.containsKey(name)) {
          return value;
        }
        int index = name.indexOf('[');
        if (index < 0 || name.charAt(name.length() - 1) != ']') {
          throw new RuntimeException("No binding found for " + name);
        }
        Object values = bindings.get(name.substring(0, index));
        if (values == null && !bindings.containsKey(name)) {
          throw new RuntimeException("No binding found for " + name);
        }
        if (!(values instanceof List)) {
          throw new RuntimeException("Value column should be list type " + name.substring(0, index));
        }
        String source = name.substring(index + 1, name.length() - 1);
        Integer indexExpr = cache.get(source);
        if (indexExpr == null) {
          int nameIndex = source.indexOf('.');
          if (nameIndex < 0) {
            boolean minus = source.charAt(0) == '_';  // cannot use '-' in identifier
            indexExpr = minus ? -Integer.valueOf(source.substring(1)) : Integer.valueOf(source);
          } else {
            Object keys = bindings.get(source.substring(0, nameIndex));
            if (!(keys instanceof List)) {
              throw new RuntimeException("Key column should be list type " + source.substring(0, nameIndex));
            }
            indexExpr = ((List) keys).indexOf(source.substring(nameIndex + 1));
            if (indexExpr < 0) {
              indexExpr = Integer.MAX_VALUE;
            }
          }
          cache.put(source, indexExpr);
        }
        List target = (List) values;
        int keyIndex = indexExpr < 0 ? target.size() + indexExpr : indexExpr;
        return keyIndex >= 0 && keyIndex < target.size() ? target.get(keyIndex) : null;
      }
    };
  }

  @JsonIgnore
  public boolean hasPostProcessing()
  {
    return pivotSpec != null || flattenSpec != null;
  }

  @Override
  public List<String> evolve(List<String> schema)
  {
    if (schema == null || hasPostProcessing()) {
      return null;
    }
    List<String> columns = Lists.newArrayList(schema);
    for (String expression : expressions) {
      String outputName = Frame.of(Evals.splitAssign(Parser.parse(expression))).getOutputName();
      if (!schema.contains(outputName)) {
        columns.add(outputName);
      }
    }
    return columns;
  }

  @Override
  public RowSignature evolve(Query query, RowSignature schema)
  {
    if (schema == null || hasPostProcessing()) {
      return null;
    }
    WindowContext context = WindowContext.newInstance(schema);
    List<String> names = Lists.newArrayList(schema.getColumnNames());
    List<ValueDesc> types = Lists.newArrayList(schema.getColumnTypes());
    for (String expression : expressions) {
      Frame frame = Frame.of(Evals.splitAssign(Parser.parse(expression, context)));
      int index = names.indexOf(frame.getOutputName());
      if (index < 0) {
        names.add(frame.getOutputName());
        types.add(frame.getOutputType());
      } else {
        names.set(index, frame.getOutputName());
        types.set(index, frame.getOutputType());
      }
    }
    return RowSignature.of(names, types);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WindowingSpec that = (WindowingSpec) o;

    if (!partitionColumns.equals(that.partitionColumns)) {
      return false;
    }
    if (!sortingColumns.equals(that.sortingColumns)) {
      return false;
    }
    if (skipSorting != that.skipSorting) {
      return false;
    }
    if (inputLimit != that.inputLimit) {
      return false;
    }
    if (!expressions.equals(that.expressions)) {
      return false;
    }
    if (!Objects.equals(flattenSpec, that.flattenSpec)) {
      return false;
    }
    if (!Objects.equals(pivotSpec, that.pivotSpec)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionColumns, sortingColumns, skipSorting, inputLimit, expressions, flattenSpec, pivotSpec);
  }

  @Override
  public String toString()
  {
    return "WindowingSpec{" +
           "skipSorting=" + skipSorting +
           (GuavaUtils.isNullOrEmpty(partitionColumns) ? "" : ", partitionColumns=" + partitionColumns) +
           (GuavaUtils.isNullOrEmpty(sortingColumns) ? "" : ", sortingColumns=" + sortingColumns) +
           (inputLimit <= 0 ? "" : ", inputLimit=" + inputLimit) +
           (GuavaUtils.isNullOrEmpty(expressions) ? "" : ", expressions=" + expressions) +
           (flattenSpec == null ? "" : ", flattenSpec=" + flattenSpec) +
           (pivotSpec == null ? "" : ", pivotSpec=" + pivotSpec) +
           '}';
  }

  public static interface PartitionEvaluatorFactory extends Cacheable
  {
    PartitionEvaluator create(WindowContext context);
  }

  public static class PartitionEvaluator
  {
    public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
    {
      return partition;
    }

    public List<Row> finalize(List<Row> partition)
    {
      return partition;
    }

    // for pivot spec
    protected List<Row> retainColumns(List<Row> partition, List<String> retainColumns)
    {
      if (GuavaUtils.isNullOrEmpty(retainColumns)) {
        return partition;
      }
      for (int i = 0; i < partition.size(); i++) {
        final Row row = partition.get(i);
        final Map<String, Object> event = new LinkedHashMap<>();
        for (String retainColumn : retainColumns) {
          if (!retainColumn.equals(Row.TIME_COLUMN_NAME)) {
            event.put(retainColumn, row.getRaw(retainColumn));
          }
        }
        partition.set(i, new MapBasedRow(row.getTimestamp(), event));
      }
      return partition;
    }
  }

  private PartitionEvaluator chain(final PartitionEvaluator... evaluators)
  {
    return new PartitionEvaluator()
    {
      @Override
      public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
      {
        for (PartitionEvaluator evaluator : evaluators) {
          partition = evaluator.evaluate(partitionKey, partition);
        }
        return partition;
      }

      @Override
      public List<Row> finalize(List<Row> partition)
      {
        for (PartitionEvaluator evaluator : evaluators) {
          partition = evaluator.finalize(partition);
        }
        return partition;
      }
    };
  }
}
