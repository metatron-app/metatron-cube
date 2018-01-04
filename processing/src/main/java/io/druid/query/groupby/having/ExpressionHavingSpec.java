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
import com.google.common.base.Predicate;
import io.druid.data.TypeResolver;
import io.druid.data.input.Row;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.RowBinding;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.List;
import java.util.Map;

/**
 */
public class ExpressionHavingSpec implements HavingSpec
{
  private final String expression;
  private final boolean finalize;

  @JsonCreator
  public ExpressionHavingSpec(
      @JsonProperty("expression") String expression,
      @JsonProperty("finalize") boolean finalize
  )
  {
    this.expression = Preconditions.checkNotNull(expression, "expression should not be null");
    this.finalize = finalize;
  }

  public ExpressionHavingSpec(String expression)
  {
    this(expression, false);
  }

  @JsonProperty("expression")
  public String getExpression()
  {
    return expression;
  }

  @Override
  public Predicate<Row> toEvaluator(TypeResolver resolver, List<AggregatorFactory> aggregators)
  {
    final Expr expr = Parser.parse(expression);
    final RowBinding binding = toBinding(resolver, aggregators);
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

  private RowBinding toBinding(final TypeResolver resolver, List<AggregatorFactory> aggregators)
  {
    final RowBinding binding;
    if (finalize && !aggregators.isEmpty()) {
      final Map<String, AggregatorFactory> factoryMap = AggregatorFactory.asMap(aggregators);
      binding = new RowBinding(resolver)
      {
        @Override
        public Object get(String name)
        {
          Object value = super.get(name);
          AggregatorFactory factory = factoryMap.get(name);
          return factory == null ? value : factory.finalizeComputation(value);
        }
      };
    } else {
      binding = new RowBinding(resolver);
    }
    return binding;
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

    return finalize == that.finalize;
  }

  @Override
  public int hashCode()
  {
    return expression.hashCode() * 31 + (finalize ? 1 : 0);
  }

  @Override
  public String toString()
  {
    return "ExpressionHavingSpec{" +
           "expression='" + expression + '\'' +
           ", finalize=" + finalize +
           '}';
  }
}
