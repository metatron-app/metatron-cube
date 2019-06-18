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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.segment.filter.Filters;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 */
public class MathExprFilter implements DimFilter
{
  private final String expression;

  @JsonCreator
  public MathExprFilter(
      @JsonProperty("expression") String expression
  )
  {
    this.expression = Preconditions.checkNotNull(expression, "expression can not be null");
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] expressionBytes = StringUtils.toUtf8(expression);
    return ByteBuffer.allocate(1 + expressionBytes.length)
                     .put(DimFilterCacheHelper.MATH_EXPR_CACHE_ID)
                     .put(expressionBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    final Expr expr = Parser.parse(expression);
    if (Evals.isConstant(expr)) {
      final boolean check = Evals.getConstantEval(expr).asBoolean();
      return check ? DimFilters.ALL : DimFilters.NONE;
    }
    return this;
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    return this;
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.addAll(Parser.findRequiredBindings(expression));
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return Filters.ofExpr(Parser.parse(expression));
  }

  @Override
  public String toString()
  {
    return "MathExprFilter{" +
           "expression='" + expression + '\'' +
           '}';
  }

  @Override
  public int hashCode()
  {
    return expression.hashCode();
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

    MathExprFilter that = (MathExprFilter) o;

    if (!expression.equals(that.expression)) {
      return false;
    }

    return true;
  }
}
