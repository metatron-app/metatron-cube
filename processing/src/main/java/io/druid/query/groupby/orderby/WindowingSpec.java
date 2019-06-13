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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.Cacheable;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.query.QueryCacheHelper;
import io.druid.query.filter.DimFilterCacheHelper;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class WindowingSpec implements Cacheable
{
  public static WindowingSpec expressions(String... expressions)
  {
    return new WindowingSpec(null, null, Arrays.asList(expressions), null, null);
  }

  private final List<String> partitionColumns;
  private final List<OrderByColumnSpec> sortingColumns;
  private final List<String> expressions;
  private final FlattenSpec flattenSpec;
  private final PivotSpec pivotSpec;

  @JsonCreator
  public WindowingSpec(
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("sortingColumns") List<OrderByColumnSpec> sortingColumns,
      @JsonProperty("expressions") List<String> expressions,
      @JsonProperty("flattenSpec") FlattenSpec flattenSpec,
      @JsonProperty("pivotSpec") PivotSpec pivotSpec
  )
  {
    this.partitionColumns = partitionColumns == null ? ImmutableList.<String>of() : partitionColumns;
    this.sortingColumns = sortingColumns == null ? ImmutableList.<OrderByColumnSpec>of() : sortingColumns;
    this.expressions = expressions == null ? ImmutableList.<String>of() : expressions;
    this.flattenSpec = flattenSpec;
    this.pivotSpec = pivotSpec;
  }

  public WindowingSpec(
      List<String> partitionColumns,
      List<OrderByColumnSpec> sortingColumns,
      List<String> expressions,
      FlattenSpec flattenSpec
  )
  {
    this(partitionColumns, sortingColumns, expressions, flattenSpec, null);
  }

  public WindowingSpec(
      List<String> partitionColumns,
      List<OrderByColumnSpec> sortingColumns,
      List<String> expressions,
      PivotSpec pivotSpec
  )
  {
    this(partitionColumns, sortingColumns, expressions, null, pivotSpec);
  }

  public WindowingSpec(
      List<String> partitionColumns,
      List<OrderByColumnSpec> sortingColumns,
      String... expressions
  )
  {
    this(partitionColumns, sortingColumns, Arrays.asList(expressions), null, null);
  }

  public WindowingSpec withoutOrdering()
  {
    return new WindowingSpec(partitionColumns, null, expressions, flattenSpec, pivotSpec);
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

  public List<OrderByColumnSpec> getPartitionOrdering()
  {
    if (partitionColumns.isEmpty()) {
      return sortingColumns;
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

  public PartitionEvaluator toEvaluator(final WindowContext context)
  {
    PartitionEvaluator evaluator = expressions.isEmpty() ? new PartitionEvaluator() : new PartitionEvaluator()
    {
      @Override
      public List<Row> evaluate(Object[] partitionKey, final List<Row> partition)
      {
        return context.with(partitionKey, partition)
                      .evaluate(Iterables.transform(expressions, new Function<String, WindowContext.Frame>()
                      {
                        @Override
                        public WindowContext.Frame apply(String expression)
                        {
                          return WindowContext.Frame.of(Evals.splitAssign(expression, context));
                        }
                      }));
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

  @Override
  public byte[] getCacheKey()
  {
    byte[] partitionColumnsBytes = QueryCacheHelper.computeCacheBytes(partitionColumns);
    byte[] sortingColumnsBytes = QueryCacheHelper.computeCacheKeys(sortingColumns);
    byte[] expressionsBytes = QueryCacheHelper.computeCacheBytes(expressions);
    byte[] flattenerBytes = QueryCacheHelper.computeCacheBytes(flattenSpec);
    byte[] pivotSpecBytes = QueryCacheHelper.computeCacheBytes(pivotSpec);

    int length = 4 + partitionColumnsBytes.length
                 + sortingColumnsBytes.length
                 + expressionsBytes.length
                 + flattenerBytes.length
                 + pivotSpecBytes.length;

    return ByteBuffer.allocate(length)
                     .put(partitionColumnsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(sortingColumnsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(expressionsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(flattenerBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(pivotSpecBytes)
                     .array();
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
    return Objects.hash(partitionColumns, sortingColumns, expressions, flattenSpec, pivotSpec);
  }

  @Override
  public String toString()
  {
    return "WindowingSpec{" +
           "partitionColumns=" + partitionColumns +
           (GuavaUtils.isNullOrEmpty(sortingColumns) ? "" : ", sortingColumns=" + sortingColumns) +
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

    protected List<Row> retainColumns(List<Row> partition, List<String> retainColumns)
    {
      if (GuavaUtils.isNullOrEmpty(retainColumns)) {
        return partition;
      }
      for (int i = 0; i < partition.size(); i++) {
        Row row = partition.get(i);
        Map<String, Object> event = new LinkedHashMap<>();
        for (String partitionColumn : retainColumns) {
          event.put(partitionColumn, row.getRaw(partitionColumn));
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
