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
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class Validation
{
  public static List<RowEvaluator<Boolean>> toEvaluators(List<Validation> validations, Map<String, ValueType> types)
  {
    if (validations == null || validations.isEmpty()) {
      return ImmutableList.of();
    }
    List<RowEvaluator<Boolean>> evaluators = Lists.newArrayList();
    for (Validation validation : validations) {
      evaluators.add(validation.toEvaluator(types));
    }
    return evaluators;
  }

  private final String columnName;
  private final List<String> exclusions;
  private final List<Expr> parsedExpressions;

  @JsonCreator
  public Validation(
      @JsonProperty("outputName") String columnName,
      @JsonProperty("exclusions") List<String> exclusions
  )
  {
    this.columnName = columnName;
    this.exclusions = Preconditions.checkNotNull(exclusions);
    this.parsedExpressions = Lists.newArrayList(
        Lists.transform(
            exclusions, new Function<String, Expr>()
            {
              @Override
              public Expr apply(String input) { return Parser.parse(input); }
            }
        )
    );
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @JsonProperty
  public List<String> getExclusions()
  {
    return exclusions;
  }

  public RowEvaluator<Boolean> toEvaluator(final Map<String, ValueType> types)
  {
    return new RowEvaluator<Boolean>()
    {
      private final RowBinding<Boolean> bindings = new RowBinding<>(columnName, types);

      @Override
      public Boolean evaluate(InputRow inputRow)
      {
        bindings.reset(inputRow);
        for (Expr expression : parsedExpressions) {
          if (expression.eval(bindings).asBoolean()) {
            return false;
          }
        }
        return true;
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

    Validation that = (Validation) o;

    if (!Objects.equals(columnName, that.columnName)) {
      return false;
    }
    if (!Objects.equals(exclusions, that.exclusions)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = columnName.hashCode();
    result = 31 * result + exclusions.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "Evaluation{" +
           "outputName='" + columnName + '\'' +
           ", exclusions=" + exclusions +
           '}';
  }
}
