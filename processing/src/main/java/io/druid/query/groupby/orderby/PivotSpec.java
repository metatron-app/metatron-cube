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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.query.QueryCacheHelper;
import io.druid.query.RowResolver;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.query.groupby.orderby.WindowContext.Evaluator;
import io.druid.query.groupby.orderby.WindowingSpec.PartitionEvaluator;
import io.druid.segment.ObjectArray;
import io.druid.segment.incremental.IncrementalIndex;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class PivotSpec implements WindowingSpec.PartitionEvaluatorFactory
{
  public static PivotSpec of(List<PivotColumnSpec> pivotColumns, String... valueColumns)
  {
    return new PivotSpec(pivotColumns, Arrays.asList(valueColumns), null, null, null, false, false);
  }

  public static PivotSpec tabular(List<PivotColumnSpec> pivotColumns, String... valueColumns)
  {
    return new PivotSpec(pivotColumns, Arrays.asList(valueColumns), null, null, null, true, false);
  }

  private final List<PivotColumnSpec> pivotColumns;
  private final List<String> valueColumns;
  private final String separator;
  private final List<String> rowExpressions;
  private final List<PartitionExpression> partitionExpressions;
  private final boolean tabularFormat;
  private final boolean appendValueColumn;

  @JsonCreator
  public PivotSpec(
      @JsonProperty("pivotColumns") List<PivotColumnSpec> pivotColumns,
      @JsonProperty("valueColumns") List<String> valueColumns,
      @JsonProperty("separator") String separator,
      @JsonProperty("rowExpressions") List<String> rowExpressions,
      @JsonProperty("partitionExpressions") List<PartitionExpression> partitionExpressions,
      @JsonProperty("tabularFormat") boolean tabularFormat,
      @JsonProperty("appendValueColumn") boolean appendValueColumn
  )
  {
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(valueColumns), "'valueColumns' cannot be null or empty");
    this.pivotColumns = pivotColumns == null ? ImmutableList.<PivotColumnSpec>of() : pivotColumns;
    this.valueColumns = valueColumns;
    this.separator = separator == null ? "-" : separator;
    this.rowExpressions = rowExpressions == null ? ImmutableList.<String>of() : rowExpressions;
    this.partitionExpressions = partitionExpressions == null
                                ? ImmutableList.<PartitionExpression>of()
                                : partitionExpressions;
    this.tabularFormat = tabularFormat;
    this.appendValueColumn = GuavaUtils.isNullOrEmpty(pivotColumns) || appendValueColumn;
  }

  public PivotSpec withSeparator(String separator)
  {
    return new PivotSpec(
        pivotColumns,
        valueColumns,
        separator,
        rowExpressions,
        partitionExpressions,
        tabularFormat,
        appendValueColumn
    );
  }

  public PivotSpec withRowExpressions(String... rowExpressions)
  {
    return new PivotSpec(
        pivotColumns,
        valueColumns,
        separator,
        Arrays.asList(rowExpressions),
        partitionExpressions,
        tabularFormat,
        appendValueColumn
    );
  }

  public PivotSpec withPartitionExpressions(PartitionExpression... partitionExpressions)
  {
    return withPartitionExpressions(Arrays.asList(partitionExpressions));
  }

  public PivotSpec withPartitionExpressions(List<PartitionExpression> partitionExpressions)
  {
    return new PivotSpec(
        pivotColumns,
        valueColumns,
        separator,
        rowExpressions,
        partitionExpressions,
        tabularFormat,
        appendValueColumn
    );
  }

  public PivotSpec withAppendValueColumn(boolean appendValueColumn)
  {
    return new PivotSpec(
        pivotColumns,
        valueColumns,
        separator,
        rowExpressions,
        partitionExpressions,
        tabularFormat,
        appendValueColumn
    );
  }

  @JsonProperty
  public List<PivotColumnSpec> getPivotColumns()
  {
    return pivotColumns;
  }

  @JsonProperty
  public List<String> getValueColumns()
  {
    return valueColumns;
  }

  @JsonProperty
  public String getSeparator()
  {
    return separator;
  }

  @JsonProperty
  public List<String> getRowExpressions()
  {
    return rowExpressions;
  }

  @JsonProperty
  public List<PartitionExpression> getPartitionExpressions()
  {
    return partitionExpressions;
  }

  @JsonProperty
  public boolean isTabularFormat()
  {
    return tabularFormat;
  }

  @JsonProperty
  public boolean isAppendValueColumn()
  {
    return appendValueColumn;
  }

  public byte[] getCacheKey()
  {
    byte[] columnsBytes = QueryCacheHelper.computeAggregatorBytes(pivotColumns);
    byte[] valuesBytes = QueryCacheHelper.computeCacheBytes(valueColumns);
    byte[] separatorBytes = QueryCacheHelper.computeCacheBytes(separator);
    byte[] rowExpressionsBytes = QueryCacheHelper.computeCacheBytes(rowExpressions);
    byte[] partitionExpressionsBytes = QueryCacheHelper.computeAggregatorBytes(partitionExpressions);

    int length = 6
                 + columnsBytes.length
                 + valuesBytes.length
                 + separatorBytes.length
                 + rowExpressionsBytes.length
                 + partitionExpressionsBytes.length;

    return ByteBuffer.allocate(length)
                     .put(columnsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(valuesBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(separatorBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(rowExpressionsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(partitionExpressionsBytes)
                     .put(tabularFormat ? (byte) 0x01 : 0)
                     .put(appendValueColumn ? (byte) 0x01 : 0)
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
    PivotSpec that = (PivotSpec) o;
    if (!pivotColumns.equals(that.pivotColumns)) {
      return false;
    }
    if (!valueColumns.equals(that.valueColumns)) {
      return false;
    }
    if (!Objects.equals(separator, that.separator)) {
      return false;
    }
    if (!Objects.equals(rowExpressions, that.rowExpressions)) {
      return false;
    }
    if (!Objects.equals(partitionExpressions, that.partitionExpressions)) {
      return false;
    }
    return tabularFormat == that.tabularFormat && appendValueColumn == that.appendValueColumn;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        pivotColumns,
        valueColumns,
        separator,
        rowExpressions,
        partitionExpressions,
        tabularFormat,
        appendValueColumn
    );
  }

  @Override
  public String toString()
  {
    return "PivotSpec{" +
           "pivotColumns=" + pivotColumns +
           ", valueColumns=" + valueColumns +
           ", separator=" + separator +
           ", rowExpressions=" + rowExpressions +
           ", partitionExpressions=" + partitionExpressions +
           ", tabularFormat=" + tabularFormat +
           ", appendValueColumn=" + appendValueColumn +
           '}';
  }

  @Override
  public PartitionEvaluator create(final WindowContext context)
  {
    if (pivotColumns.isEmpty()) {
      // just for metatron.. I hate this
      final List<Evaluator> rowEvals = PartitionExpression.toEvaluators(PartitionExpression.from(rowExpressions));
      final List<Evaluator> partitionEvals = PartitionExpression.toEvaluators(partitionExpressions);
      // it's actually not pivoting.. so group-by columns are valid (i don't know why other metrics should be removed)
      final List<String> retainColumns = GuavaUtils.dedupConcat(
          context.partitionColumns(), WindowContext.outputNames(rowEvals), WindowContext.outputNames(partitionEvals),
          valueColumns
      );
      if (rowEvals.isEmpty() && partitionEvals.isEmpty()) {
        return new PartitionEvaluator(retainColumns);
      }
      return new PartitionEvaluator(retainColumns)
      {
        @Override
        public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
        {
          return super.evaluate(partitionKey, context.with(partition).evaluate(rowEvals, valueColumns));
        }

        @Override
        public List<Row> finalize(List<Row> partition)
        {
          return super.finalize(context.with(partition).evaluate(partitionEvals, valueColumns));
        }
      };
    }
    final List<String> partitionColumns = context.partitionColumns();
    final List<Function<Row, String>> extractors = PivotColumnSpec.toExtractors(pivotColumns);
    // this is name ordering of pivot columns.. not for row ordering
    final List<Comparator<String>> comparators = OrderByColumnSpec.getComparator(pivotColumns);
    if (appendValueColumn) {
      comparators.add(Ordering.<String>explicit(valueColumns));
    }
    final Set[] whitelist = PivotColumnSpec.getValuesAsArray(pivotColumns);
    final String[] values = valueColumns.toArray(new String[0]);
    final Comparator<StringArray> comparator = new Comparator<StringArray>()
    {
      @Override
      public int compare(StringArray o1, StringArray o2)
      {
        final String[] array1 = o1.array();
        final String[] array2 = o2.array();
        for (int i = 0; i < comparators.size(); i++) {
          int compare = comparators.get(i).compare(array1[i], array2[i]);
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };
    // '_' is just '_'
    final List<Pair<String, Expr>> rowExprs = Lists.newArrayList();
    for (String expression : rowExpressions) {
      rowExprs.add(Evals.splitAssign(expression));
    }
    // when tabularFormat = true, '_' is replaced with pivot column names
    final List<Evaluator> pivotExprs = PartitionExpression.toEvaluators(partitionExpressions);
    final int keyLength = pivotColumns.size() + (appendValueColumn ? 1 : 0);

    final Set<StringArray> whole = Sets.newHashSet();
    return new PartitionEvaluator()
    {
      @Override
      @SuppressWarnings("unchecked")
      public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
      {
        final Map<StringArray, Object> mapping = Maps.newHashMap();
        final DateTime dateTime = partition.get(0).getTimestamp();

next:
        for (Row row : partition) {
          String[] array = new String[keyLength];
          for (int i = 0; i < extractors.size(); i++) {
            array[i] = Objects.toString(extractors.get(i).apply(row));
            if (whitelist[i] != null && !whitelist[i].contains(array[i])) {
              continue next;
            }
          }
          if (appendValueColumn) {
            for (int i = 0; i < values.length; i++) {
              if (i > 0) {
                array = Arrays.copyOf(array, array.length);
              }
              array[extractors.size()] = values[i];
              StringArray key = new StringArray(array);
              Object value = row.getRaw(values[i]);
              Preconditions.checkArgument(mapping.put(key, value) == null, "duplicated.. " + key);
            }
          } else {
            StringArray key = new StringArray(array);
            Object value;
            if (values.length == 1) {
              value = row.getRaw(values[0]);
            } else {
              Object[] holder = new Object[values.length];
              for (int x = 0; x < holder.length; x++) {
                holder[x] = row.getRaw(values[x]);
              }
              value = Arrays.asList(holder);
            }
            Preconditions.checkArgument(mapping.put(key, value) == null, "duplicated.. " + key);
          }
        }
        Map<String, Object> event = Maps.newLinkedHashMap();
        for (int i = 0; i < partitionKey.length; i++) {
          event.put(partitionColumns.get(i), partitionKey[i]);
        }
        Collection<Map.Entry<StringArray, Object>> entries;
        if (tabularFormat) {
          entries = mapping.entrySet();
        } else {
          entries = IncrementalIndex.sortOn(mapping, comparator, false);
        }
        for (Map.Entry<StringArray, Object> entry : entries) {
          String newKey = StringUtils.concat(separator, entry.getKey().array());
          Object newValue = entry.getValue();
          event.put(newKey, newValue);
          if (newValue != null) {
            context.addType(newKey, RowResolver.toValueType(newValue));
          }
        }
        Expr.NumericBinding binding = WindowingSpec.withMap(event);
        for (Pair<String, Expr> assign : rowExprs) {
          event.put(assign.lhs, assign.rhs.eval(binding).value());
        }
        if (tabularFormat) {
          whole.addAll(mapping.keySet());
        }
        return Arrays.<Row>asList(new MapBasedRow(dateTime, event));
      }

      @Override
      public List<Row> finalize(List<Row> partition)
      {
        // handle single partition
        WindowContext current = context.on(null, null).with(partition);
        if (tabularFormat) {
          StringArray[] keys = whole.toArray(new StringArray[whole.size()]);
          Arrays.parallelSort(keys, comparator);
          final String[] sortedKeys = new String[keys.length];
          for (int i = 0; i < sortedKeys.length; i++) {
            sortedKeys[i] = StringUtils.concat(separator, keys[i].array());
          }
          // rewrite with whole pivot columns
          for (int i = 0; i < partition.size(); i++) {
            Row row = partition.get(i);
            Map<String, Object> event = new LinkedHashMap<>();
            for (String partitionColumn : partitionColumns) {
              event.put(partitionColumn, row.getRaw(partitionColumn));
            }
            for (String sortedKey : sortedKeys) {
              event.put(sortedKey, row.getRaw(sortedKey));
            }
            for (Pair<String, Expr> assign : rowExprs) {
              event.put(assign.lhs, row.getRaw(assign.lhs));
            }
            partition.set(i, new MapBasedRow(row.getTimestamp(), event));
          }
          current.evaluate(pivotExprs, Arrays.asList(sortedKeys));
        } else {
          current.evaluate(pivotExprs);
        }
        return partition;
      }
    };
  }

  private static class StringArray extends ObjectArray<String>
  {
    public StringArray(String[] array)
    {
      super(array);
    }
  }
}
