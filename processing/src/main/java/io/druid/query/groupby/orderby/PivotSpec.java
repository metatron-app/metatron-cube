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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.ExprEval;
import io.druid.query.QueryCacheHelper;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.query.groupby.orderby.WindowContext.Frame;
import io.druid.query.groupby.orderby.WindowContext.FrameFactory;
import io.druid.query.groupby.orderby.WindowingSpec.PartitionEvaluator;
import io.druid.segment.StringArray;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Arrays;
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
    return new PivotSpec(pivotColumns, Arrays.asList(valueColumns), null, null, null, null, false, false);
  }

  public static PivotSpec tabular(List<PivotColumnSpec> pivotColumns, String... valueColumns)
  {
    return new PivotSpec(pivotColumns, Arrays.asList(valueColumns), null, null, null, null, true, false);
  }

  private final List<PivotColumnSpec> pivotColumns;
  private final List<String> valueColumns;
  private final String separator;
  private final String nullValue;
  private final List<String> rowExpressions;
  private final List<PartitionExpression> partitionExpressions;
  private final boolean tabularFormat;  // I really hate this
  private final boolean appendValueColumn;

  @JsonCreator
  public PivotSpec(
      @JsonProperty("pivotColumns") List<PivotColumnSpec> pivotColumns,
      @JsonProperty("valueColumns") List<String> valueColumns,
      @JsonProperty("separator") String separator,
      @JsonProperty("nullColumnName") String nullValue,
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
    this.nullValue = nullValue == null ? "" : nullValue;
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
        nullValue,
        rowExpressions,
        partitionExpressions,
        tabularFormat,
        appendValueColumn
    );
  }

  public PivotSpec withNullValue(String nullValue)
  {
    return new PivotSpec(
        pivotColumns,
        valueColumns,
        separator,
        nullValue,
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
        nullValue,
        Arrays.asList(rowExpressions),
        partitionExpressions,
        tabularFormat,
        appendValueColumn
    );
  }

  public PivotSpec withPartitionExpressions(String... partitionExpressions)
  {
    return withPartitionExpressions(PartitionExpression.from(Arrays.asList(partitionExpressions)));
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
        nullValue,
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
        nullValue,
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
  public String getNullValue()
  {
    return nullValue;
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

  @Override
  public byte[] getCacheKey()
  {
    byte[] columnsBytes = QueryCacheHelper.computeCacheKeys(pivotColumns);
    byte[] valuesBytes = QueryCacheHelper.computeCacheBytes(valueColumns);
    byte[] separatorBytes = QueryCacheHelper.computeCacheBytes(separator);
    byte[] nullValueBytes = QueryCacheHelper.computeCacheBytes(nullValue);
    byte[] rowExpressionsBytes = QueryCacheHelper.computeCacheBytes(rowExpressions);
    byte[] partitionExpressionsBytes = QueryCacheHelper.computeCacheKeys(partitionExpressions);

    int length = 7
                 + columnsBytes.length
                 + valuesBytes.length
                 + separatorBytes.length
                 + nullValueBytes.length
                 + rowExpressionsBytes.length
                 + partitionExpressionsBytes.length;

    return ByteBuffer.allocate(length)
                     .put(columnsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(valuesBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(separatorBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(nullValueBytes)
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
    if (!Objects.equals(nullValue, that.nullValue)) {
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
        nullValue,
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
           (StringUtils.isNullOrEmpty(nullValue) ? "" : ", nullValue=" + nullValue) +
           (GuavaUtils.isNullOrEmpty(rowExpressions) ? "" : ", rowExpressions='" + rowExpressions + '\'') +
           (GuavaUtils.isNullOrEmpty(partitionExpressions) ? "" : ", partitionExpressions='" + partitionExpressions + '\'') +
           ", tabularFormat=" + tabularFormat +
           ", appendValueColumn=" + appendValueColumn +
           '}';
  }

  private static Iterable<Frame> toEvaluators(
      final Iterable<PartitionExpression> partitionExpressions,
      final WindowContext context,
      final List<String> columns
  )
  {
    return JodaUtils.explode(partitionExpressions, new Function<PartitionExpression, Iterable<Frame>>()
    {
      @Override
      public Iterable<Frame> apply(PartitionExpression expression)
      {
        return new FrameFactory(expression.getCondition(), expression.getExpression()).intialize(context, columns);
      }
    });
  }

  @Override
  public PartitionEvaluator create(final WindowContext context)
  {
    if (pivotColumns.isEmpty()) {
      // just for metatron.. I hate this
      final List<String> removeColumns = GuavaUtils.exclude(context.getExcludedColumns(), valueColumns);
      if (rowExpressions.isEmpty() && partitionExpressions.isEmpty()) {
        return new PartitionEvaluator()
        {
          @Override
          public List<Row> finalize(List<Row> partition)
          {
            return retainColumns(partition, GuavaUtils.exclude(context.getOutputColumns(), removeColumns));
          }
        };
      }
      // it's actually not pivoting.. so group-by columns are valid (i don't know why other metrics should be removed)
      return new PartitionEvaluator()
      {
        @Override
        public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
        {
          return context.with(partitionKey, partition).evaluate(
              toEvaluators(PartitionExpression.from(rowExpressions), context, valueColumns)
          );
        }

        @Override
        public List<Row> finalize(List<Row> partition)
        {
          List<Row> evaluated = context.with(null, partition).evaluate(
              toEvaluators(partitionExpressions, context, valueColumns)
          );
          return retainColumns(evaluated, GuavaUtils.exclude(context.getOutputColumns(), removeColumns));
        }
      };
    }
    final List<String> partitionColumns = context.partitionColumns();
    final List<Function<Row, String>> extractors = PivotColumnSpec.toExtractors(pivotColumns, nullValue);

    final Set[] whitelist = PivotColumnSpec.getValuesAsArray(pivotColumns);
    final String[] values = valueColumns.toArray(new String[0]);
    final ValueDesc[] valueTypes = Lists.transform(valueColumns, context).toArray(new ValueDesc[0]);
    final ValueDesc structType = ValueDesc.ofStruct(values, valueTypes);

    final int keyLength = pivotColumns.size() + (appendValueColumn ? 1 : 0);

    final Set<StringArray> allPivotColumns = Sets.newHashSet();
    final Set<String> allColumns = Sets.newHashSet();
    return new PartitionEvaluator()
    {
      @Override
      @SuppressWarnings("unchecked")
      public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
      {
        final Map<StringArray, ExprEval> mapping = Maps.newHashMap();
        final DateTime dateTime = partition.get(0).getTimestamp();

next:
        for (Row row : partition) {
          String[] array = new String[keyLength];
          for (int i = 0; i < extractors.size(); i++) {
            array[i] = extractors.get(i).apply(row);
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
              ExprEval value = ExprEval.of(row.getRaw(values[i]), valueTypes[i]);
              Preconditions.checkArgument(mapping.put(key, value) == null, "duplicated.. " + key);
            }
          } else {
            StringArray key = new StringArray(array);
            ExprEval value;
            if (values.length == 1) {
              value = ExprEval.of(row.getRaw(values[0]), valueTypes[0]);
            } else {
              Object[] holder = new Object[values.length];
              for (int x = 0; x < holder.length; x++) {
                holder[x] = row.getRaw(values[x]);
              }
              value = ExprEval.of(Arrays.asList(holder), structType);
            }
            Preconditions.checkArgument(mapping.put(key, value) == null, "duplicated.. " + key);
          }
        }
        PivotContext pivot = new PivotContext(PivotSpec.this, context.with(partitionKey, partition));
        Map<String, Object> event = pivot.evaluate(mapping);
        if (tabularFormat) {
          allPivotColumns.addAll(mapping.keySet());
          allColumns.addAll(event.keySet());
        }
        return Arrays.<Row>asList(new MapBasedRow(dateTime, event));
      }

      @Override
      public List<Row> finalize(List<Row> partition)
      {
        // handle single partition
        WindowContext current = context.with(null, partition);
        if (tabularFormat) {
          StringArray[] pivotColumns = allPivotColumns.toArray(new StringArray[0]);
          Arrays.sort(pivotColumns, makeColumnOrdering());
          final String[] sortedKeys = new String[pivotColumns.length];
          for (int i = 0; i < sortedKeys.length; i++) {
            sortedKeys[i] = StringUtils.concat(separator, pivotColumns[i].array());
          }
          allColumns.removeAll(partitionColumns);
          allColumns.removeAll(Arrays.asList(sortedKeys));
          String[] remainingColumns = allColumns.toArray(new String[0]);
          Arrays.sort(remainingColumns);
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
            for (String sortedKey : remainingColumns) {
              event.put(sortedKey, row.getRaw(sortedKey));
            }
            partition.set(i, new MapBasedRow(row.getTimestamp(), event));
          }
          // '_' is replaced with pivot column names
          current.evaluate(toEvaluators(partitionExpressions, context, Arrays.asList(sortedKeys)));
        } else {
          final List<WindowContext.Frame> frames = Lists.newArrayList();
          for (PartitionExpression expression : partitionExpressions) {
            frames.add(WindowContext.Frame.of(Evals.splitAssign(expression.getExpression(), context)));
          }
          current.evaluate(frames);
        }
        return partition;
      }
    };
  }

  // this is name ordering of pivot columns.. not for row ordering
  public Comparator<StringArray> makeColumnOrdering()
  {
    final List<Comparator<String>> comparators = OrderByColumnSpec.getComparator(pivotColumns);
    if (appendValueColumn) {
      comparators.add(Ordering.<String>explicit(valueColumns));
    }
    return new Comparator<StringArray>()
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
  }
}
