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

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.data.ValueType;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class Evaluation
{
  public static List<RowEvaluator<InputRow>> toEvaluators(List<Evaluation> evaluations, Map<String, ValueType> types)
  {
    if (evaluations == null || evaluations.isEmpty()) {
      return ImmutableList.of();
    }
    List<RowEvaluator<InputRow>> evaluators = Lists.newArrayList();
    for (Evaluation evaluation : evaluations) {
      evaluators.add(evaluation.toEvaluator(types));
    }
    return evaluators;
  }

  private final String outputName;
  private final List<String> expressions;
  private final List<Expr> parsedExpressions;

  @JsonCreator
  public Evaluation(
      @JsonProperty("outputName") String outputName,
      @JsonProperty("expressions") List<String> expressions
  )
  {
    this.outputName = Preconditions.checkNotNull(outputName);
    this.expressions = Preconditions.checkNotNull(expressions);
    this.parsedExpressions = Lists.newArrayList(
        Lists.transform(
            expressions, new Function<String, Expr>()
            {
              @Override
              public Expr apply(String input) { return Parser.parse(input); }
            }
        )
    );
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

  public RowEvaluator<InputRow> toEvaluator(final Map<String, ValueType> types)
  {
    return new RowEvaluator<InputRow>()
    {
      private final RowBinding<Object> bindings = new RowBinding<>(outputName, types);

      @Override
      public InputRow evaluate(InputRow inputRow)
      {
        bindings.reset(inputRow);
        for (Expr expression : parsedExpressions) {
          bindings.set(expression.eval(bindings).value());
        }
        // hate this (todo)
        ((MapBasedRow)inputRow).getEvent().put(outputName, bindings.get());
        return inputRow;
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
