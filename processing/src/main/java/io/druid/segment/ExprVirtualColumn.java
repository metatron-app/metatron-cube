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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Parser;
import io.druid.query.extraction.ExtractionFn;

import java.util.Objects;

/**
 */
public class ExprVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x01;

  private final String outputName;
  private final String expression;

  @JsonCreator
  public ExprVirtualColumn(
      @JsonProperty("expression") String expression,
      @JsonProperty("outputName") String outputName
  )
  {
    this.expression = Preconditions.checkNotNull(expression, "expression should not be null");
    this.outputName = outputName == null ? Iterables.getOnlyElement(
        Parser.findRequiredBindings(expression), "output name should not be null") : outputName;
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
  public DimensionSelector asDimension(String dimension, ExtractionFn extractionFn, ColumnSelectorFactory factory)
  {
    return VirtualColumns.toDimensionSelector(asMetric(dimension, factory), extractionFn);
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    Preconditions.checkArgument(column.equals(outputName));
    return Parser.parse(expression, Parser.withTypes(types)).returns();
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new ExprVirtualColumn(expression, outputName);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(VC_TYPE_ID)
                  .append(expression, outputName);
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
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
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expression, outputName);
  }

  @Override
  public String toString()
  {
    return "ExprVirtualColumn{" +
           "expression='" + expression + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
