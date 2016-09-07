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
import com.metamx.common.StringUtils;
import io.druid.query.filter.DimFilterCacheHelper;

import java.nio.ByteBuffer;
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
    Preconditions.checkArgument(expression != null, "expression should not be null");
    Preconditions.checkArgument(outputName != null, "output name should not be null");

    this.expression = expression;
    this.outputName = outputName;
  }

  @Override
  public ObjectColumnSelector asMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.wrapAsObjectSelector(Object.class, factory.makeMathExpressionSelector(expression));
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
  public VirtualColumn duplicate()
  {
    return new ExprVirtualColumn(expression, outputName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] expr = StringUtils.toUtf8(expression);
    byte[] output = StringUtils.toUtf8(outputName);

    return ByteBuffer.allocate(3 + expr.length + output.length)
                     .put(VC_TYPE_ID)
                     .put(expr).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(output)
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
