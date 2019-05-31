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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 */
public class Validation
{
  static final Logger LOG = new Logger(Validation.class);

  public static List<RowEvaluator<Boolean>> toValidators(List<Validation> validations, TypeResolver resolver)
  {
    if (GuavaUtils.isNullOrEmpty(validations)) {
      return ImmutableList.of();
    }
    List<Expr> parsedExpressions = Lists.newArrayList();
    List<RowEvaluator<Boolean>> validators = Lists.newArrayList();
    for (Validation validation : validations) {
      if (validation.columnName == null) {
        parsedExpressions.addAll(validation.parsedExpressions);
        continue;
      }
      if (!parsedExpressions.isEmpty()) {
        validators.add(toEvaluator(resolver, null, parsedExpressions));
        parsedExpressions = Lists.newArrayList();
      }
      validators.add(validation.toEvaluator(resolver));
    }
    if (!parsedExpressions.isEmpty()) {
      validators.add(toEvaluator(resolver, null, parsedExpressions));
    }
    return validators;
  }

  private static RowEvaluator<Boolean> toEvaluator(
      final TypeResolver resolver,
      final String columnName,
      final List<Expr> expressions
  )
  {
    final InputRowBinding<Boolean> bindings = new InputRowBinding<>(columnName, resolver);
    return new RowEvaluator<Boolean>()
    {
      @Override
      public Boolean evaluate(InputRow inputRow)
      {
        bindings.reset(inputRow);
        for (Expr expression : expressions) {
          if (expression.eval(bindings).asBoolean()) {
            return false;
          }
        }
        return true;
      }
    };
  }

  private final String columnName;
  private final List<String> exclusions;
  @JsonIgnore
  private final List<Expr> parsedExpressions;

  @JsonCreator
  public Validation(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("exclusion") String exclusion,
      @JsonProperty("exclusions") List<String> exclusions
  )
  {
    this.columnName = columnName;
    Preconditions.checkArgument(
        exclusion == null ^ exclusions == null,
        "Must have either expression or expressions"
    );
    this.exclusions = exclusion == null ? exclusions : Arrays.asList(exclusion);
    this.parsedExpressions = Lists.newArrayList(
        Lists.transform(
            this.exclusions, new Function<String, Expr>()
            {
              @Override
              public Expr apply(String input) { return Parser.parse(input); }
            }
        )
    );
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getColumnName()
  {
    return columnName;
  }

  @JsonProperty
  public List<String> getExclusions()
  {
    return exclusions;
  }

  public RowEvaluator<Boolean> toEvaluator(TypeResolver resolver)
  {
    return toEvaluator(resolver, columnName, parsedExpressions);
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
    return Objects.hash(columnName, exclusions);
  }

  @Override
  public String toString()
  {
    return "Validation{" +
           "columnName='" + columnName + '\'' +
           ", exclusions=" + exclusions +
           '}';
  }
}
