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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 */
public class Evaluation
{
  public static List<RowEvaluator<InputRow>> toEvaluators(List<Evaluation> evaluations, TypeResolver resolver)
  {
    if (GuavaUtils.isNullOrEmpty(evaluations)) {
      return ImmutableList.of();
    }
    List<RowEvaluator<InputRow>> evaluators = Lists.newArrayList();
    for (Evaluation evaluation : evaluations) {
      evaluators.add(evaluation.toEvaluator(resolver));
    }
    return evaluators;
  }

  private final String outputName;
  private final List<String> expressions;

  @JsonCreator
  public Evaluation(
      @JsonProperty("outputName") String outputName,
      @JsonProperty("expression") String expression,
      @JsonProperty("expressions") List<String> expressions
  )
  {
    this.outputName = Preconditions.checkNotNull(outputName);
    Preconditions.checkArgument(
        expression == null ^ expressions == null,
        "Must have either expression or expressions"
    );
    this.expressions = expression == null ? expressions : Arrays.asList(expression);
  }

  public Evaluation(String outputName, String expression)
  {
    this(outputName, expression, null);
  }

  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @JsonProperty
  public List<String> getExpressions()
  {
    return expressions;
  }

  public RowEvaluator<InputRow> toEvaluator(final TypeResolver resolver)
  {
    final InputRowBinding<Object> bindings = new InputRowBinding<>(outputName, resolver);
    final List<Expr> parsedExpressions = Lists.newArrayList(Lists.transform(
        expressions, new Function<String, Expr>()
        {
          @Override
          public Expr apply(String input) { return Parser.parse(input, resolver); }
        }
    ));

    return new RowEvaluator<InputRow>()
    {
      @Override
      public InputRow evaluate(InputRow inputRow)
      {
        bindings.reset(inputRow);
        for (Expr expression : parsedExpressions) {
          bindings.set(expression.eval(bindings).value());
        }
        Row.Updatable updatable = Rows.toUpdatable(inputRow);
        updatable.set(outputName, bindings.get());
        return (InputRow) updatable;
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

    Evaluation that = (Evaluation) o;

    if (!Objects.equals(outputName, that.outputName)) {
      return false;
    }
    if (!Objects.equals(expressions, that.expressions)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = outputName.hashCode();
    result = 31 * result + expressions.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "Evaluation{" +
           "outputName='" + outputName + '\'' +
           ", expressions=" + expressions +
           '}';
  }
}
