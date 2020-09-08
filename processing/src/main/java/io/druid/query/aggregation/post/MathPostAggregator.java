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

package io.druid.query.aggregation.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class MathPostAggregator extends PostAggregator.Stateless implements PostAggregator.Decorating
{
  private final String name;
  private final String expression;
  private final String ordering;
  private final boolean finalize;

  private final Comparator comparator;

  public MathPostAggregator(String expression)
  {
    this(null, expression, true, null);
  }

  public MathPostAggregator(String name, String expression)
  {
    this(name, expression, true, null);
  }

  @JsonCreator
  public MathPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("expression") String expression,
      @JsonProperty("finalize") boolean finalize,
      @JsonProperty("ordering") String ordering
  )
  {
    this.expression = Preconditions.checkNotNull(expression, "'expression' cannot not be null");
    if (name == null) {
      Expr expr = Parser.parse(expression);
      Preconditions.checkArgument(Evals.isAssign(expr), "should be assign expression if name is null");
      this.name = Evals.splitSimpleAssign(expression).lhs;
    } else {
      this.name = name;
    }
    this.finalize = finalize;
    this.ordering = ordering;
    this.comparator = ordering == null ? GuavaUtils.nullFirstNatural() : NumericOrdering.valueOf(ordering);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(Parser.findRequiredBindings(expression));
  }

  @Override
  public Comparator getComparator()
  {
    return comparator;
  }

  private static Expr parseExpr(String expression, TypeResolver resolver)
  {
    Expr expr = Parser.parse(expression, resolver);
    if (Evals.isAssign(expr)) {
      expr = Evals.splitSimpleAssign(expression).rhs;
    }
    return expr;
  }

  @Override
  protected Processor createStateless()
  {
    return new AbstractProcessor()
    {
      private final Expr parsed = parseExpr(expression, TypeResolver.UNKNOWN);

      @Override
      public Object compute(DateTime timestamp, Map<String, Object> values)
      {
        return parsed.eval(Parser.withTimeAndMap(timestamp, values)).value();
      }
    };
  }

  @Override
  public ValueDesc resolve(TypeResolver resolver)
  {
    return parseExpr(expression, resolver).returns();
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty("expression")
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty("ordering")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getOrdering()
  {
    return ordering;
  }

  @JsonProperty
  public boolean isFinalize()
  {
    return finalize;
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    if (finalize && !GuavaUtils.isNullOrEmpty(aggregators)) {
      return new Finalizing(name, expression, ordering, aggregators);
    }
    return this;
  }

  public enum NumericOrdering implements Comparator<Number>
  {
    // ensures the following order: numeric > NaN > Infinite
    numericFirst {
      public int compare(Number lhs, Number rhs)
      {
        if (lhs instanceof Long && rhs instanceof Long) {
          return Long.compare(lhs.longValue(), rhs.longValue());
        }
        double d1 = lhs.doubleValue();
        double d2 = rhs.doubleValue();
        if (isFinite(d1) && !isFinite(d2)) {
          return 1;
        }
        if (!isFinite(d1) && isFinite(d2)) {
          return -1;
        }
        return Double.compare(d1, d2);
      }

      // Double.isFinite only exist in JDK8
      private boolean isFinite(double value)
      {
        return !Double.isInfinite(value) && !Double.isNaN(value);
      }
    }
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

    MathPostAggregator that = (MathPostAggregator) o;

    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(expression, that.expression)) {
      return false;
    }
    if (finalize != that.finalize) {
      return false;
    }
    if (!Objects.equals(ordering, that.ordering)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, expression, finalize, ordering);
  }

  @Override
  public String toString()
  {
    return "MathPostAggregator{" +
           "name='" + name + '\'' +
           ", expression='" + expression + '\'' +
           ", finalize=" + finalize +
           (ordering == null ? "" : ", ordering=" + ordering) +
           '}';
  }

  private static class Finalizing extends MathPostAggregator
  {
    private final Map<String, AggregatorFactory> aggregators;

    public Finalizing(String name, String expression, String ordering, Map<String, AggregatorFactory> aggregators)
    {
      super(name, expression, false, ordering);
      this.aggregators = aggregators;
    }

    @Override
    public Processor createStateless()
    {
      return new AbstractProcessor()
      {
        private final Expr parsed = parseExpr(getExpression(), TypeResolver.UNKNOWN);

        @Override
        public Object compute(DateTime timestamp, Map<String, Object> values)
        {
          final Expr.NumericBinding binding = new Expr.NumericBinding()
          {
            @Override
            public Collection<String> names()
            {
              return values.keySet();
            }

            @Override
            public Object get(final String name)
            {
              if (name.equals(Column.TIME_COLUMN_NAME)) {
                return timestamp;
              }
              final Object value = values.get(name);
              if (value == null) {
                return null;
              }
              AggregatorFactory factory = aggregators.get(name);
              return factory == null ? value : factory.finalizeComputation(value);
            }
          };
          return parsed.eval(binding).value();
        }
      };
    }
  }
}
