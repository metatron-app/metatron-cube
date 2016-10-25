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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
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

/**
 */
public class FlattenSpec implements Cacheable
{
  private final Flattener type;
  private final List<String> columns;
  private final String suffixColumn;
  private final List<String> expressions;

  @JsonCreator
  public FlattenSpec(
      @JsonProperty("type") Flattener type,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("suffixColumn") String suffixColumn,
      @JsonProperty("expressions") List<String> expressions
  )
  {
    this.type = type;
    this.columns = columns;
    this.suffixColumn = suffixColumn;
    this.expressions = expressions == null ? ImmutableList.<String>of() : expressions;
    Preconditions.checkArgument(columns != null && !columns.isEmpty(), "'columns' should not be null or empty");
  }

  public FlattenSpec(Flattener type, List<String> columns)
  {
    this(type, columns, null, null);
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
  public String getSuffixColumn()
  {
    return suffixColumn;
  }

  public byte[] getCacheKey()
  {
    byte[] typeBytes = StringUtils.toUtf8WithNullToEmpty(getType());
    byte[] columnsBytes = QueryCacheHelper.computeCacheBytes(columns);
    byte[] suffixColumnBytes = StringUtils.toUtf8WithNullToEmpty(suffixColumn);

    int length = 3 + typeBytes.length + columnsBytes.length + suffixColumnBytes.length;

    return ByteBuffer.allocate(length)
                     .put(typeBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(columnsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(suffixColumnBytes)
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
    if (!Objects.equals(suffixColumn, that.suffixColumn)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + columns.hashCode();
    result = 31 * result + Objects.hashCode(suffixColumn);
    return result;
  }

  public PartitionEvaluator toEvaluator(List<String> partitionColumns, List<OrderByColumnSpec> sortingColumns)
  {
    return type.toEvaluator(partitionColumns, sortingColumns, this);
  }

  @Override
  public String toString()
  {
    return "FlattenSpec{" +
           ", type='" + type + '\'' +
           ", columns=" + columns +
           ", suffixColumn='" + suffixColumn + '\'' +
           '}';
  }

  private int[] toIndex(List<String> partitionColumns) {
    List<Integer> evaluateTable = Lists.newArrayList();
    for (String column : columns) {
      evaluateTable.add(partitionColumns.indexOf(column));
    }
    return Ints.toArray(evaluateTable);
  }

  public static enum Flattener
  {
    EXPLODE {
      @Override
      public PartitionEvaluator toEvaluator(
          final List<String> partitionColumns,
          final List<OrderByColumnSpec> sortingColumns,
          final FlattenSpec spec)
      {
        final int[] index = spec.toIndex(partitionColumns);
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
              String postFix = spec.suffixColumn == null ? "." + i : "." + row.getRaw(spec.suffixColumn);
              for (int j = 0; j < index.length; j++) {
                if (index[j] < 0) {
                  final String column = spec.columns.get(j);
                  flatten.put(column + postFix, row.getRaw(column));
                }
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
          final FlattenSpec spec)
      {
        final int[] index = spec.toIndex(partitionColumns);
        final List<Pair<String, Expr>> assigns = Lists.newArrayList();
        for (String expression : spec.expressions) {
          assigns.add(Evals.splitAssign(expression));
        }
        return new PartitionEvaluator()
        {
          @Override
          @SuppressWarnings("unchecked")
          public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
          {
            final List[] array = new List[index.length];
            for (int i = 0; i < index.length; i++) {
              if (index[i] < 0) {
                array[i] = Lists.newArrayListWithCapacity(partition.size());
              }
            }
            DateTime dateTime = null;
            for (Row row : partition) {
              for (int i = 0; i < index.length; i++) {
                if (index[i] < 0) {
                  array[i].add(row.getRaw(spec.columns.get(i)));
                }
              }
              if (dateTime == null) {
                dateTime = row.getTimestamp();
              }
            }
            Map<String, Object> flatten = Maps.newLinkedHashMap();
            for (int i = 0; i < partitionKey.length; i++) {
              flatten.put(partitionColumns.get(i), partitionKey[i]);
            }
            for (int i = 0; i < index.length; i++) {
              if (index[i] < 0) {
                flatten.put(spec.columns.get(i), array[i]);
              }
            }
            if (!assigns.isEmpty()) {
              Expr.NumericBinding binding = withMap(flatten);
              for (Pair<String, Expr> assign : assigns) {
                flatten.put(assign.lhs, assign.rhs.eval(binding).value());
              }
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
      return name == null ? EXPLODE : valueOf(name.toUpperCase());
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
          keyIndex = ((List)keys).indexOf(keyValue);
          cache.put(source, keyIndex);
        }
        return keyIndex < 0 ? null : ((List)values).get(keyIndex);
      }
    };
  }
}
