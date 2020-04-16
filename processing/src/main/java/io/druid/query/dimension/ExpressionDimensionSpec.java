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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.RowResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.util.Map;

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
  public ValueDesc resolve(final Supplier<? extends TypeResolver> resolver)
  {
    return Parser.parse(expression, resolver.get()).returns();
  }

  @Override
  public ExtractionFn getExtractionFn()
  {
    return null;
  }

  @Override
  public DimensionSelector decorate(final DimensionSelector selector, final TypeResolver resolver)
  {
    final String dimension = getDimension();
    final Expr expr = Parser.parse(expression, resolver);
    final ValueDesc resultType = expr.returns();
    if (!Comparable.class.isAssignableFrom(RowResolver.toClass(resultType))) {
      throw new IllegalArgumentException("cannot wrap as dimension selector for type " + resultType);
    }
    final Map<String, Object> mapping = Maps.newHashMap();
    final Expr.NumericBinding binding = Parser.withMap(mapping);
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
        mapping.put(dimension, selector.lookupName(id));
        return (Comparable) expr.eval(binding).value();
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
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(expression);
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
