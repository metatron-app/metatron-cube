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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.metamx.common.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Parser;
import io.druid.query.filter.DimFilterCacheHelper;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 */
public class ExprVirtualColumn implements VirtualColumn.Generic
{
  private static final byte VC_TYPE_ID = 0x01;

  private final String outputName;
  private final String expression;

  private final boolean includeAsDimension;
  private final boolean includeAsMetric;

  @JsonCreator
  public ExprVirtualColumn(
      @JsonProperty("expression") String expression,
      @JsonProperty("outputName") String outputName,
      @JsonProperty("includeAsDimension") boolean includeAsDimension,
      @JsonProperty("includeAsMetric") boolean includeAsMetric
  )
  {
    this.expression = Preconditions.checkNotNull(expression, "expression should not be null");
    this.outputName = outputName == null ? Iterables.getOnlyElement(
        Parser.findRequiredBindings(expression), "output name should not be null") : outputName;
    Preconditions.checkArgument(
        !(includeAsDimension && includeAsMetric),
        "Must have a valid, non-null fieldName or fieldExpression"
    );
    this.includeAsDimension = includeAsDimension;
    this.includeAsMetric = includeAsMetric;
  }

  public ExprVirtualColumn(String expression, String outputName)
  {
    this(expression, outputName, false, false);
  }

  @Override
  public ObjectColumnSelector asMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.wrapAsObjectSelector(factory.makeMathExpressionSelector(expression));
  }

  @Override
  public FloatColumnSelector asFloatMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.wrapAsFloatSelector(factory.makeMathExpressionSelector(expression));
  }

  @Override
  public DoubleColumnSelector asDoubleMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.wrapAsDoubleSelector(factory.makeMathExpressionSelector(expression));
  }

  @Override
  public LongColumnSelector asLongMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.wrapAsLongSelector(factory.makeMathExpressionSelector(expression));
  }

  @Override
  public DimensionSelector asDimension(String dimension, ColumnSelectorFactory factory)
  {
    return VirtualColumns.toDimensionSelector(asMetric(dimension, factory));
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    Preconditions.checkArgument(column.equals(outputName));
    return Parser.parse(expression).type(Parser.withTypes(types)).asValueDesc();
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new ExprVirtualColumn(expression, outputName, includeAsDimension, includeAsMetric);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] expr = StringUtils.toUtf8(expression);
    byte[] output = StringUtils.toUtf8(outputName);

    return ByteBuffer.allocate(2 + expr.length + output.length + 2)
                     .put(VC_TYPE_ID)
                     .put(expr).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(output)
                     .put(includeAsDimension ? (byte) 1 : 0)
                     .put(includeAsMetric ? (byte) 1 : 0)
                     .array();
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  @JsonProperty
  public boolean includeAsDimension()
  {
    return includeAsDimension;
  }

  @Override
  @JsonProperty
  public boolean includeAsMetric()
  {
    return includeAsMetric;
  }

  @Override
  public boolean isIndexed(String dimension)
  {
    return false;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExprVirtualColumn)) {
      return false;
    }

    ExprVirtualColumn that = (ExprVirtualColumn) o;

    if (!expression.equals(that.expression)) {
      return false;
    }
    if (!outputName.equals(that.outputName)) {
      return false;
    }
    if (includeAsDimension != that.includeAsDimension) {
      return false;
    }
    if (includeAsMetric != that.includeAsMetric) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expression, outputName, includeAsDimension, includeAsMetric);
  }

  @Override
  public String toString()
  {
    return "ExprVirtualColumn{" +
           "expression='" + expression + '\'' +
           ", outputName='" + outputName + '\'' +
           ", includeAsDimension='" + includeAsDimension + '\'' +
           ", includeAsMetric='" + includeAsMetric + '\'' +
           '}';
  }
}
