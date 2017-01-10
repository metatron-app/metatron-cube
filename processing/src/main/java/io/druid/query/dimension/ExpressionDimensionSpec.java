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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metamx.common.StringUtils;
import io.druid.math.expr.Parser;
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;

/**
 */
public class ExpressionDimensionSpec implements DimensionSpec
{
  private static final byte CACHE_TYPE_ID = 0x5;

  private final String expression;
  private final String outputName;

  @JsonCreator
  public ExpressionDimensionSpec(
      @JsonProperty("expression") String expression,
      @JsonProperty("outputName") String outputName
  )
  {
    this.expression = expression;
    this.outputName = outputName;
  }

  @Override
  public String getDimension()
  {
    return Iterables.getOnlyElement(Parser.findRequiredBindings(expression));
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
  public ExtractionFn getExtractionFn()
  {
    final Function<String, String> function = Parser.asStringFunction(Parser.parse(expression));
    return new DimExtractionFn()
    {
      @Override
      public byte[] getCacheKey()
      {
        throw new IllegalStateException("should not be called");
      }

      @Override
      public String apply(String value)
      {
        return function.apply(value);
      }

      @Override
      public boolean preservesOrdering()
      {
        return false;
      }

      @Override
      public ExtractionType getExtractionType()
      {
        return ExtractionType.MANY_TO_ONE;
      }
    };
  }

  @Override
  public DimensionSelector decorate(DimensionSelector selector)
  {
    return selector;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] expressionBytes = StringUtils.toUtf8(expression);

    return ByteBuffer.allocate(1 + expressionBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(expressionBytes)
                     .array();
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "ExpressionDimensionSpec{" +
        "expression='" + expression + '\'' +
        ", outputName='" + outputName + '\'' +
        '}';
  }
}
