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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.query.QueryCacheHelper;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.query.groupby.orderby.WindowingSpec.PartitionEvaluator;
import io.druid.query.ordering.StringComparator;
import io.druid.segment.ObjectArray;
import io.druid.segment.incremental.IncrementalIndex;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class PivotSpec implements WindowingSpec.PartitionEvaluatorFactory
{
  private final List<PivotColumnSpec> pivotColumns;
  private final List<String> valueColumns;
  private final String separator;
  private final List<String> expressions;

  @JsonCreator
  public PivotSpec(
      @JsonProperty("pivotColumns") List<PivotColumnSpec> pivotColumns,
      @JsonProperty("valueColumns") List<String> valueColumns,
      @JsonProperty("separator") String separator,
      @JsonProperty("expressions") List<String> expressions
  )
  {
    this.pivotColumns = Preconditions.checkNotNull(pivotColumns);
    this.valueColumns = Preconditions.checkNotNull(valueColumns);
    this.separator = separator == null ? "-" : separator;
    this.expressions = expressions == null ? ImmutableList.<String>of() : expressions;
    Preconditions.checkArgument(!pivotColumns.isEmpty(), "'columns' should not be null or empty");
    Preconditions.checkArgument(!valueColumns.isEmpty(), "'values' should not be null or empty");
  }

  public PivotSpec(List<PivotColumnSpec> pivotColumns, List<String> valueColumns)
  {
    this(pivotColumns, valueColumns, null, null);
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
  public List<String> getExpressions()
  {
    return expressions;
  }

  public byte[] getCacheKey()
  {
    byte[] columnsBytes = QueryCacheHelper.computeAggregatorBytes(pivotColumns);
    byte[] valuesBytes = QueryCacheHelper.computeCacheBytes(valueColumns);
    byte[] separatorBytes = QueryCacheHelper.computeCacheBytes(separator);
    byte[] expressionsBytes = QueryCacheHelper.computeCacheBytes(expressions);

    int length = 3
                 + columnsBytes.length
                 + valuesBytes.length
                 + separatorBytes.length
                 + expressionsBytes.length;

    return ByteBuffer.allocate(length)
                     .put(columnsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(valuesBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(separatorBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(expressionsBytes)
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
    if (!Objects.equals(expressions, that.expressions)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(pivotColumns, valueColumns, separator, expressions);
  }

  @Override
  public String toString()
  {
    return "PivotSpec{" +
           "pivotColumns=" + pivotColumns +
           ", valueColumns=" + valueColumns +
           ", separator=" + separator +
           ", expressions=" + expressions +
           '}';
  }

  public PivotSpec withExpression(String... expressions)
  {
    return new PivotSpec(pivotColumns, valueColumns, separator, Arrays.asList(expressions));
  }

  @Override
  public PartitionEvaluator create(final List<String> partitionColumns, final List<OrderByColumnSpec> sortingColumns)
  {
    final String[] columns = OrderByColumnSpec.getColumnsAsArray(pivotColumns);
    final StringComparator[] comparators = OrderByColumnSpec.getComparatorAsArray(pivotColumns);
    final Set[] whitelist = PivotColumnSpec.getValuesAsArray(pivotColumns);
    final String[] values = valueColumns.toArray(new String[0]);
    final Comparator<StringArray> comparator = new Comparator<StringArray>()
    {
      @Override
      public int compare(StringArray o1, StringArray o2)
      {
        final String[] array1 = o1.array();
        final String[] array2 = o2.array();
        for (int i = 0; i < comparators.length; i++) {
          int compare = comparators[i].compare(array1[i], array2[i]);
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };
    final List<Pair<String, Expr>> assigns = Lists.newArrayList();
    for (String expression : expressions) {
      assigns.add(Evals.splitAssign(expression));
    }
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
          String[] array = new String[columns.length];
          for (int i = 0; i < array.length; i++) {
            array[i] = Objects.toString(row.getRaw(columns[i]), "");
            if (whitelist[i] != null && !whitelist[i].contains(array[i])) {
              continue next;
            }
          }
          StringArray key = new StringArray(array);
          Object value;
          if (values.length == 1) {
            value = row.getRaw(values[0]);
          } else {
            Object[] holder = new Object[values.length];
            for (int x = 0; x < holder.length; x++) {
              holder[x] = row.getRaw(values[x]);
            }
            value = holder;
          }
          Preconditions.checkArgument(mapping.put(key, value) == null, "duplicated.. " + key);
        }
        Map<String, Object> event = Maps.newLinkedHashMap();
        for (int i = 0; i < partitionKey.length; i++) {
          event.put(partitionColumns.get(i), partitionKey[i]);
        }
        for (Map.Entry<StringArray, Object> entry : IncrementalIndex.sortOn(mapping, comparator, false)) {
          event.put(StringUtils.concat(separator, entry.getKey().array()), entry.getValue());
        }
        if (!assigns.isEmpty()) {
          Expr.NumericBinding binding = WindowingSpec.withMap(event);
          for (Pair<String, Expr> assign : assigns) {
            event.put(assign.lhs, assign.rhs.eval(binding).value());
          }
        }
        return Arrays.<Row>asList(new MapBasedRow(dateTime, event));
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
