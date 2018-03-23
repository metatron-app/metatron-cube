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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.QueryCacheHelper;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.OrderingSpec;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class PivotColumnSpec extends OrderingSpec
{
  public static PivotColumnSpec ofColumn(
      String dimension,
      Direction direction,
      String comparatorName,
      List<String> values)
  {
    return new PivotColumnSpec(dimension, null, direction, comparatorName, values);
  }

  public static PivotColumnSpec ofExpression(
      String expression,
      Direction direction,
      String comparatorName,
      List<String> values)
  {
    return new PivotColumnSpec(null, expression, direction, comparatorName, values);
  }

  public static PivotColumnSpec of(String column)
  {
    return new PivotColumnSpec(column, null);
  }

  public static PivotColumnSpec of(OrderByColumnSpec column)
  {
    return new PivotColumnSpec(
        column.getDimension(),
        null,
        column.getDirection(),
        column.getDimensionOrder(),
        null
    );
  }

  public static List<PivotColumnSpec> toSpecs(String... columns)
  {
    List<PivotColumnSpec> columnSpecs = Lists.newArrayList();
    for (String column : columns) {
      columnSpecs.add(create(column));
    }
    return columnSpecs;
  }

  public static List<Function<Row, String>> toExtractors(List<PivotColumnSpec> pivotColumnSpecs)
  {
    List<Function<Row, String>> extractors = Lists.newArrayList();
    for (PivotColumnSpec columnSpec : pivotColumnSpecs) {
      extractors.add(columnSpec.toExtractor());
    }
    return extractors;
  }

  public static List<Set<String>> getValues(List<PivotColumnSpec> pivotColumnSpecs)
  {
    List<Set<String>> comparators = Lists.newArrayList();
    for (PivotColumnSpec columnSpec : pivotColumnSpecs) {
      comparators.add(GuavaUtils.isNullOrEmpty(columnSpec.values) ? null : Sets.newHashSet(columnSpec.values));
    }
    return comparators;
  }

  public static Set[] getValuesAsArray(List<PivotColumnSpec> pivotColumnSpecs)
  {
    return getValues(pivotColumnSpecs).toArray(new Set[pivotColumnSpecs.size()]);
  }

  @JsonCreator
  public static PivotColumnSpec create(Object obj)
  {
    if (obj == null) {
      return null;
    } else if (obj instanceof String) {
      return new PivotColumnSpec(obj.toString(), null, null, null, null);
    } else if (obj instanceof Map) {
      final Map map = (Map) obj;

      final String dimension = Objects.toString(map.get("dimension"), null);
      final String expression = Objects.toString(map.get("expression"), null);
      final Direction direction = Direction.fromString(Objects.toString(map.get("direction"), null));
      final String dimensionOrder = Objects.toString(map.get("dimensionOrder"), null);

      return new PivotColumnSpec(dimension, expression, direction, dimensionOrder, (List) map.get("values"));
    } else {
      throw new ISE("Cannot build an PivotColumnSpec from a %s", obj.getClass());
    }
  }

  private final String dimension;
  private final String expression;
  private final List<String> values;

  public PivotColumnSpec(
      String dimension,
      String expression,
      Direction direction,
      String comparatorName,
      List<String> values
  )
  {
    super(direction, comparatorName);
    this.dimension = dimension;
    this.expression = expression;
    this.values = values;
    Preconditions.checkArgument(
        dimension == null ^ expression == null,
        "Must have a valid, non-null dimension or expression"
    );
  }

  public PivotColumnSpec(String dimension, List<String> values)
  {
    this(dimension, null, null, null, values);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty
  public List<String> getValues()
  {
    return values;
  }

  public Function<Row, String> toExtractor()
  {
    if (dimension != null) {
      return new Function<Row, String>()
      {
        @Override
        public String apply(Row input)
        {
          return Objects.toString(input.getRaw(dimension), "");
        }
      };
    } else {
      final Expr expr = Parser.parse(expression);
      final DSuppliers.HandOver<Row> supplier = new DSuppliers.HandOver<>();
      final Expr.NumericBinding binding = Parser.withRowSupplier(supplier);
      return new Function<Row, String>()
      {
        @Override
        public String apply(Row input)
        {
          supplier.set(input);
          return expr.eval(binding).asString();
        }
      };
    }
  }

  @Override
  public String toString()
  {
    return "PivotColumnSpec{" +
           "dimension='" + dimension + '\'' +
           ", expression='" + expression + '\'' +
           ", dimensionOrder='" + getDimensionOrder() + '\'' +
           ", direction=" + getDirection() + '\'' +
           ", values='" + values + '\'' +
           '}';
  }

  @Override
  public int hashCode()
  {
    return super.hashCode() * 31 + Objects.hashCode(values);
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || !(o instanceof PivotColumnSpec)) {
      return false;
    }
    PivotColumnSpec other = (PivotColumnSpec) o;
    return super.equals(o) &&
           Objects.equals(dimension, other.dimension) &&
           Objects.equals(expression, other.expression) &&
           Objects.equals(values, other.values);
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] superBytes = super.getCacheKey();
    final byte[] dimensionBytes = QueryCacheHelper.computeCacheBytes(dimension);
    final byte[] expressionBytes = QueryCacheHelper.computeCacheBytes(expression);
    final byte[] valuesBytes = QueryCacheHelper.computeCacheBytes(values);

    return ByteBuffer.allocate(superBytes.length + dimensionBytes.length + expressionBytes.length + valuesBytes.length)
                     .put(superBytes)
                     .put(dimensionBytes)
                     .put(expressionBytes)
                     .put(valuesBytes)
                     .array();
  }
}
