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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import io.druid.data.TypeResolver;
import io.druid.data.input.Row;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.RowBinding;

/**
 */
public class ExpressionHavingSpec implements HavingSpec
{
  private final String expression;

  @JsonCreator
  public ExpressionHavingSpec(
      @JsonProperty("expression") String expression
  )
  {
    this.expression = Preconditions.checkNotNull(expression, "expression should not be null");
  }

  @JsonProperty("expression")
  public String getExpression()
  {
    return expression;
  }

  @Override
  public Predicate<Row> toEvaluator(TypeResolver resolver)
  {
    final Expr expr = Parser.parse(expression, resolver);
    final RowBinding binding = new RowBinding(resolver);
    return new Predicate<Row>()
    {
      @Override
      public boolean apply(Row input)
      {
        binding.reset(input);
        return expr.eval(binding).asBoolean();
      }
    };
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

    ExpressionHavingSpec that = (ExpressionHavingSpec) o;

    if (!expression.equals(that.expression)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return expression.hashCode();
  }

  @Override
  public String toString()
  {
    return "ExpressionHavingSpec{" +
           "expression='" + expression + '\'' +
           '}';
  }
}
