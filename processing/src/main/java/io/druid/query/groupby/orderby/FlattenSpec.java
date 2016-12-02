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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.query.Cacheable;
import io.druid.query.QueryCacheHelper;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.query.groupby.orderby.WindowingSpec.PartitionEvaluator;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class FlattenSpec implements Cacheable
{
  public static FlattenSpec array(List<String> columns, String separator)
  {
    return new FlattenSpec(Flattener.ARRAY, columns, null, null, separator, null);
  }

  public static FlattenSpec array(
      List<String> pivotColumns,
      List<String> prefixColumns,
      List<String> columns,
      String separator
  )
  {
    return new FlattenSpec(Flattener.ARRAY, columns, pivotColumns, prefixColumns, separator, null);
  }

  public static FlattenSpec expand(List<String> columns, String separator)
  {
    return new FlattenSpec(Flattener.EXPAND, columns, null, null, separator, null);
  }

  public static FlattenSpec expand(
      List<String> pivotColumns,
      List<String> prefixColumns,
      List<String> columns,
      String separator
  )
  {
    return new FlattenSpec(Flattener.EXPAND, columns, pivotColumns, prefixColumns, separator, null);
  }

  private final Flattener type;
  private final List<String> columns;
  private final List<String> pivotColumns;
  private final List<String> prefixColumns;
  private final String separator;
  private final List<String> expressions;

  @JsonCreator
  public FlattenSpec(
      @JsonProperty("type") Flattener type,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("pivotColumns") List<String> pivotColumns,
      @JsonProperty("prefixColumns") List<String> prefixColumns,
      @JsonProperty("separator") String separator,
      @JsonProperty("expressions") List<String> expressions
  )
  {
    this.type = type;
    this.columns = columns;
    this.pivotColumns = pivotColumns;
    this.separator = separator;
    this.prefixColumns = prefixColumns;
    this.expressions = expressions == null ? ImmutableList.<String>of() : expressions;
    Preconditions.checkArgument(columns != null && !columns.isEmpty(), "'columns' should not be null or empty");
  }

  @JsonProperty
  public String getType()
  {
    return type.name();
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public List<String> getPivotColumns()
  {
    return pivotColumns;
  }

  @JsonProperty
  public List<String> getPrefixColumns()
  {
    return prefixColumns;
  }

  @JsonProperty
  public String getSeparator()
  {
    return separator;
  }

  @JsonProperty
  public List<String> getExpressions()
  {
    return expressions;
  }

  public byte[] getCacheKey()
  {
    byte[] typeBytes = StringUtils.toUtf8WithNullToEmpty(getType());
    byte[] columnsBytes = QueryCacheHelper.computeCacheBytes(columns);
    byte[] pivotColumnsBytes = QueryCacheHelper.computeCacheBytes(pivotColumns);
    byte[] prefixColumnsBytes = QueryCacheHelper.computeCacheBytes(prefixColumns);
    byte[] separatorBytes = StringUtils.toUtf8WithNullToEmpty(separator);
    byte[] expressionsBytes = QueryCacheHelper.computeCacheBytes(expressions);

    int length = 5
                 + typeBytes.length
                 + columnsBytes.length
                 + pivotColumnsBytes.length
                 + prefixColumnsBytes.length
                 + separatorBytes.length
                 + expressionsBytes.length;

    return ByteBuffer.allocate(length)
                     .put(typeBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(columnsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(pivotColumnsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(prefixColumnsBytes)
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
    FlattenSpec that = (FlattenSpec) o;
    if (type != that.type) {
      return false;
    }
    if (!columns.equals(that.columns)) {
      return false;
    }
    if (!Objects.equals(pivotColumns, that.pivotColumns)) {
      return false;
    }
    if (!Objects.equals(separator, that.separator)) {
      return false;
    }
    if (!Objects.equals(prefixColumns, that.prefixColumns)) {
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
    return Objects.hash(type, columns, pivotColumns, prefixColumns, separator, expressions);
  }

  @Override
  public String toString()
  {
    return "FlattenSpec{" +
           "type='" + type + '\'' +
           ", columns=" + columns +
           ", pivotColumns=" + pivotColumns +
           ", prefixColumns=" + prefixColumns +
           ", separator=" + separator +
           ", expressions=" + expressions +
           '}';
  }

  public FlattenSpec withExpression(String... expressions)
  {
    return new FlattenSpec(type, columns, pivotColumns, prefixColumns, separator, Arrays.asList(expressions));
  }

  public PartitionEvaluator toEvaluator(List<String> partitionColumns, List<OrderByColumnSpec> sortingColumns)
  {
    return type.toEvaluator(partitionColumns, sortingColumns, this);
  }

  private String[] getColumnsExcept(List<String> partitionColumns)
  {
    List<String> excluded = Lists.newArrayList(columns);
    excluded.removeAll(partitionColumns);
    return excluded.toArray(new String[excluded.size()]);
  }

  public static enum Flattener
  {
    EXPAND {
      @Override
      public PartitionEvaluator toEvaluator(
          final List<String> partitionColumns,
          final List<OrderByColumnSpec> sortingColumns,
          final FlattenSpec spec
      )
      {
        final String[] columns = spec.getColumnsExcept(partitionColumns);
        final boolean prefixed = spec.prefixColumns != null && !spec.prefixColumns.isEmpty();
        return new PartitionEvaluator()
        {
          @Override
          public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
          {
            DateTime dateTime = null;
            final Map<String, Object> flatten = Maps.newLinkedHashMap();
            for (int i = 0; i < partitionKey.length; i++) {
              flatten.put(partitionColumns.get(i), partitionKey[i]);
            }
            for (int i = 0; i < partition.size(); i++) {
              Row row = partition.get(i);
              String prefix;
              if (prefixed) {
                StringBuilder b = new StringBuilder();
                for (String prefixColumn : spec.prefixColumns) {
                  b.append(row.getRaw(prefixColumn)).append(spec.separator);
                }
                prefix = b.toString();
              } else {
                prefix = i + spec.separator;
              }
              for (String column : columns) {
                flatten.put(prefix + column, row.getRaw(column));
              }
              if (dateTime == null) {
                dateTime = row.getTimestamp();
              }
            }
            return Arrays.<Row>asList(new MapBasedRow(dateTime, flatten));
          }
        };
      }
    },
    ARRAY {
      @Override
      public PartitionEvaluator toEvaluator(
          final List<String> partitionColumns,
          final List<OrderByColumnSpec> sortingColumns,
          final FlattenSpec spec
      )
      {
        final String[] columns = spec.getColumnsExcept(partitionColumns);
        final List<Pair<String, Expr>> assigns = Lists.newArrayList();
        for (String expression : spec.expressions) {
          assigns.add(Evals.splitAssign(expression));
        }
        final String separator = spec.separator == null ? "-" : spec.separator;
        final boolean pivotRow = spec.pivotColumns != null && !spec.pivotColumns.isEmpty();

        final List<String> pivotColumns;
        if (pivotRow && spec.prefixColumns != null && !spec.prefixColumns.isEmpty()) {
          pivotColumns = Lists.newArrayList(spec.pivotColumns);
          pivotColumns.removeAll(spec.prefixColumns);
        } else {
          pivotColumns = spec.pivotColumns;
        }
        final StringBuilder b = new StringBuilder();

        if (pivotRow && spec.prefixColumns != null && !spec.prefixColumns.isEmpty()) {
          return new PartitionEvaluator()
          {

            @Override
            @SuppressWarnings("unchecked")
            public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
            {
              final Map<ObjectArray, List[]> prefixed = Maps.newHashMap();
              final Set rows = Sets.newLinkedHashSet();
              final DateTime dateTime = partition.get(0).getTimestamp();
              for (Row row : partition) {
                ObjectArray key = new ObjectArray(new Object[spec.prefixColumns.size()]);
                for (int i = 0; i < key.length(); i++) {
                  key.array[i] = row.getRaw(spec.prefixColumns.get(i));
                }
                List[] array = prefixed.get(key);
                if (array == null) {
                  prefixed.put(key, array = new List[columns.length]);
                  for (int i = 0; i < columns.length; i++) {
                    array[i] = Lists.newArrayListWithCapacity(partition.size());
                  }
                }
                for (int i = 0; i < columns.length; i++) {
                  array[i].add(row.getRaw(columns[i]));
                }
                for (String pivotColumn : pivotColumns) {
                  if (b.length() > 0) {
                    b.append(separator);
                  }
                  b.append(row.getRaw(pivotColumn));
                }
                String pivot = b.toString();
                if (!rows.contains(pivot)) {
                  rows.add(pivot);
                }
                b.setLength(0);
              }
              Map<String, Object> flatten = Maps.newLinkedHashMap();
              for (int i = 0; i < partitionKey.length; i++) {
                flatten.put(partitionColumns.get(i), partitionKey[i]);
              }
              for (Map.Entry<ObjectArray, List[]> entry : prefixed.entrySet()) {
                ObjectArray prefix = entry.getKey();
                List[] array = entry.getValue();
                for (int i = 0; i < columns.length; i++) {
                  flatten.put(prefix.concat(spec.separator, columns[i]), array[i]);
                }
              }
              if (!assigns.isEmpty()) {
                Expr.NumericBinding binding = withMap(flatten);
                for (Pair<String, Expr> assign : assigns) {
                  flatten.put(assign.lhs, assign.rhs.eval(binding).value());
                }
              }
              Map<String, Object> event = ImmutableMap.of("rows", Lists.newArrayList(rows), "columns", flatten);
              return Arrays.<Row>asList(new MapBasedRow(dateTime, event));
            }
          };
        }
        return new PartitionEvaluator()
        {
          @Override
          @SuppressWarnings("unchecked")
          public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
          {
            final List[] array = new List[columns.length];
            for (int i = 0; i < columns.length; i++) {
              array[i] = Lists.newArrayListWithCapacity(partition.size());
            }
            final List rows = pivotRow ? Lists.newArrayListWithCapacity(partition.size()) : null;
            final DateTime dateTime = partition.get(0).getTimestamp();
            for (Row row : partition) {
              for (int i = 0; i < columns.length; i++) {
                array[i].add(row.getRaw(columns[i]));
              }
              if (pivotRow) {
                for (String pivotColumn : pivotColumns) {
                  if (b.length() > 0) {
                    b.append(separator);
                  }
                  b.append(row.getRaw(pivotColumn));
                }
                rows.add(b.toString());
                b.setLength(0);
              }
            }
            Map<String, Object> flatten = Maps.newLinkedHashMap();
            for (int i = 0; i < partitionKey.length; i++) {
              flatten.put(partitionColumns.get(i), partitionKey[i]);
            }
            for (int i = 0; i < columns.length; i++) {
              flatten.put(columns[i], array[i]);
            }
            if (!assigns.isEmpty()) {
              Expr.NumericBinding binding = withMap(flatten);
              for (Pair<String, Expr> assign : assigns) {
                flatten.put(assign.lhs, assign.rhs.eval(binding).value());
              }
            }
            if (pivotRow) {
              flatten = ImmutableMap.of("rows", rows, "columns", flatten);
            }
            return Arrays.<Row>asList(new MapBasedRow(dateTime, flatten));
          }
        };
      }
    };

    @Override
    @JsonValue
    public String toString()
    {
      return name();
    }

    @JsonCreator
    public static Flattener fromString(String name)
    {
      return name == null ? ARRAY : valueOf(name.toUpperCase());
    }

    public abstract PartitionEvaluator toEvaluator(
        List<String> partitionColumns,
        List<OrderByColumnSpec> sortingColumns,
        FlattenSpec spec
    );
  }

  private static Expr.NumericBinding withMap(final Map<String, ?> bindings)
  {
    return new Expr.NumericBinding()
    {
      private final Map<String, Integer> cache = Maps.newHashMap();

      @Override
      public Object get(String name)
      {
        Object value = bindings.get(name);
        if (value != null || bindings.containsKey(name)) {
          return value;
        }
        int index = name.lastIndexOf('.');
        if (index < 0) {
          throw new RuntimeException("No binding found for " + name);
        }
        Object values = bindings.get(name.substring(0, index));
        if (values == null && !bindings.containsKey(name)) {
          throw new RuntimeException("No binding found for " + name);
        }
        if (!(values instanceof List)) {
          throw new RuntimeException("Value column should be list type " + name.substring(0, index));
        }
        String source = name.substring(index + 1);
        Integer keyIndex = cache.get(source);
        if (keyIndex == null) {
          int nameIndex = source.indexOf('[');
          if (nameIndex < 0 || source.charAt(source.length() - 1) != ']') {
            throw new RuntimeException("Invalid source format " + source);
          }
          Object keys = bindings.get(source.substring(0, nameIndex));
          if (!(keys instanceof List)) {
            throw new RuntimeException("Key column should be list type " + source.substring(0, nameIndex));
          }
          String keyValue = source.substring(nameIndex + 1, source.length() - 1);
          keyIndex = ((List) keys).indexOf(keyValue);
          cache.put(source, keyIndex);
        }
        return keyIndex < 0 ? null : ((List) values).get(keyIndex);
      }
    };
  }

  private static class ObjectArray
  {
    private final Object[] array;

    private ObjectArray(Object[] array)
    {
      this.array = array;
    }

    public int length()
    {
      return array.length;
    }

    public String concat(String delimiter, String postfix)
    {
      StringBuilder b = new StringBuilder();
      for (Object element : array) {
        if (b.length() > 0) {
          b.append(delimiter);
        }
        b.append(element);
      }
      return b.append(delimiter).append(postfix).toString();
    }

    @Override
    public boolean equals(Object o)
    {
      return o instanceof ObjectArray && Arrays.equals(array, ((ObjectArray) o).array);
    }

    @Override
    public int hashCode()
    {
      return Arrays.hashCode(array);
    }

    @Override
    public String toString()
    {
      return Arrays.toString(array);
    }
  }
}
