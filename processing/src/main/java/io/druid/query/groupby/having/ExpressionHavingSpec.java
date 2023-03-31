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
import io.druid.common.IntTagged;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.RowBinding;
import io.druid.query.RowSignature;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class ExpressionHavingSpec implements HavingSpec.PostMergeSupport
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
  public Predicate<Row> toEvaluator(RowSignature signature)
  {
    RowBinding binding = new RowBinding(signature);
    Expr expr = binding.optimize(Parser.parse(expression, signature));
    return input -> Evals.evalBoolean(expr, binding.reset(input));
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
  public Predicate<Row> toCompactEvaluator(RowSignature signature)
  {
    Expr parsed = Parser.parse(expression, signature);
    List<String> bindings = Parser.findRequiredBindings(parsed);
    if (!GuavaUtils.containsAll(signature.getColumnNames(), bindings)) {
      return null;
    }
    RowBinding binding = new RowBinding(signature)
    {
      private final Mapping mapping = new Mapping(signature.columnToIndexAndType(), bindings);

      @Override
      protected Object getValue(Row row, String name)
      {
        IntTagged<ValueDesc> index = mapping.get(name);
        if (index == null) {
          return null;
        }
        Object value = ((CompactRow) row).valueAt(index.tag);
        if (value instanceof String && index.value.isPrimitiveNumeric()) {
          return index.value.type().castIfPossible(value);
        }
        return value;
      }
    };
    Expr expr = binding.optimize(parsed);
    return input -> Evals.evalBoolean(expr, binding.reset(input));
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

  private static class Mapping
  {
    private final List<String> keys;
    private final List<IntTagged<ValueDesc>> values;

    private Mapping(Map<String, IntTagged<ValueDesc>> mapping, List<String> keys)
    {
      this.keys = keys;
      this.values = keys.stream().map(mapping::get).collect(Collectors.toList());
    }

    private IntTagged<ValueDesc> get(String name)
    {
      return values.get(keys.indexOf(name));
    }
  }
}
