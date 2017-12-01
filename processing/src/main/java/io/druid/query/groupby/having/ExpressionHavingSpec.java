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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.data.input.Row;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;

import java.util.Collection;

/**
 */
public class ExpressionHavingSpec implements HavingSpec
{
  private final String expression;

  private final Expr expr;
  private final RowBinding binding;

  @JsonCreator
  public ExpressionHavingSpec(@JsonProperty("expression") String expression)
  {
    this.expression = Preconditions.checkNotNull(expression, "expression should not be null");
    this.expr = Parser.parse(expression);
    this.binding = new RowBinding();
  }

  @JsonProperty("expression")
  public String getExpression()
  {
    return expression;
  }

  @Override
  public boolean eval(Row row)
  {
    return expr.eval(binding.setRow(row)).asBoolean();
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

  private static class RowBinding implements Expr.NumericBinding
  {
    private Row row;

    @Override
    public Collection<String> names()
    {
      return row.getColumns();
    }

    @Override
    public Object get(String name)
    {
      return row.getRaw(name);
    }

    private RowBinding setRow(Row row)
    {
      this.row = row;
      return this;
    }
  }
}
