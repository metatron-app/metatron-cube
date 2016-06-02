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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.StringUtils;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class ExpressionExtractionFn implements ExtractionFn
{
  @JsonProperty
  private final String expression;

  private final Expr parsed;
  private final HandOver supplier;
  private final Expr.NumericBinding bindings;

  @JsonCreator
  public ExpressionExtractionFn(@JsonProperty("expression") String expression)
  {
    this.expression = Preconditions.checkNotNull(expression, "expression must not be null");

    Expr exprs = Parser.parse(expression);
    List<String> binding = Parser.findRequiredBindings(exprs);
    if (binding.size() > 1) {
      throw new IllegalArgumentException("cannot handle multiple dimensions " + binding);
    }
    parsed = exprs;
    supplier = new HandOver();
    if (!binding.isEmpty()) {
      bindings = Parser.withSuppliers(ImmutableMap.<String, Supplier<Object>>of(binding.get(0), supplier));
    } else {
      bindings = null;
    }
  }

  @Nullable
  @Override
  public String apply(String key)
  {
    return apply((Object) key);
  }

  @Nullable
  @Override
  public String apply(Object value)
  {
    supplier.value = value;
    return parsed.eval(bindings).asString();
  }

  @Nullable
  @Override
  public String apply(long value)
  {
    return apply((Object) value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionFn.ExtractionType getExtractionType()
  {
    return ExtractionFn.ExtractionType.MANY_TO_ONE;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] expressionBytes = StringUtils.toUtf8(expression);
    return ByteBuffer.allocate(2 + expressionBytes.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_EXPRESSION)
                     .put((byte) 0XFF)
                     .put(expressionBytes)
                     .array();
  }

  private static class HandOver implements Supplier<Object>
  {
    private Object value;

    @Override
    public Object get()
    {
      return value;
    }
  }
}
