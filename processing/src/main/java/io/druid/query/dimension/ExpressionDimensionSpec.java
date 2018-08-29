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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.metamx.common.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.RowResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;

/**
 */
public class ExpressionDimensionSpec implements DimensionSpec
{
  public static DimensionSpec of(String expression, String outputName)
  {
    return new ExpressionDimensionSpec(expression, outputName);
  }

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
  public ValueDesc resolve(final TypeResolver resolver)
  {
    return Parser.parse(expression).resolve(
        new TypeResolver.Abstract()
        {
          @Override
          public ValueDesc resolve(String name)
          {
            return resolver.resolve(name, ValueDesc.UNKNOWN);
          }
        }
    );
  }

  @Override
  public ExtractionFn getExtractionFn()
  {
    return null;
  }

  @Override
  public DimensionSelector decorate(final DimensionSelector selector)
  {
    final String dimension = getDimension();
    final TypeResolver bindings = Parser.withTypeMap(
        ImmutableMap.<String, ValueDesc>of(dimension, selector.type())
    );
    final Expr expr = Parser.parse(expression);
    final ValueDesc resultType = expr.resolve(bindings);
    if (!Comparable.class.isAssignableFrom(RowResolver.toClass(resultType))) {
      throw new IllegalArgumentException("cannot wrap as dimension selector for type " + resultType);
    }
    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        return selector.getRow();
      }

      @Override
      public int getValueCardinality()
      {
        return selector.getValueCardinality();    // todo
      }

      @Override
      public Comparable lookupName(int id)
      {
        return (Comparable) expr.eval(
            Parser.withMap(ImmutableMap.<String, Object>of(dimension, selector.lookupName(id)))
        ).value();
      }

      @Override
      public ValueDesc type()
      {
        return resultType;
      }

      @Override
      public int lookupId(Comparable name)
      {
        throw new UnsupportedOperationException("lookupId");
      }
    };
  }

  @Override
  public DimensionSpec withOutputName(String outputName)
  {
    return new ExpressionDimensionSpec(expression, outputName);
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
  public String getDescription()
  {
    return expression;
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
