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

package io.druid.segment;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;

import java.util.Map;

/**
 * Factory class for MetricSelectors
 */
public interface ColumnSelectorFactory extends TypeResolver
{
  public Iterable<String> getColumnNames();
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec);
  public FloatColumnSelector makeFloatColumnSelector(String columnName);
  public DoubleColumnSelector makeDoubleColumnSelector(String columnName);
  public LongColumnSelector makeLongColumnSelector(String columnName);
  public <T> ObjectColumnSelector<T> makeObjectColumnSelector(String columnName);
  public ExprEvalColumnSelector makeMathExpressionSelector(String expression);
  public ExprEvalColumnSelector makeMathExpressionSelector(Expr expression);
  public ValueMatcher makePredicateMatcher(DimFilter filter);

  abstract class Predicate implements ColumnSelectorFactory
  {
    @Override
    public ValueMatcher makePredicateMatcher(DimFilter filter)
    {
      return filter.toFilter(this).makeMatcher(this);
    }

    @Override
    public ValueDesc resolve(String column, ValueDesc defaultType)
    {
      return Optional.fromNullable(resolve(column)).or(defaultType);
    }
  }

  abstract class ExprSupport extends Predicate
  {
    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(final String expression)
    {
      return makeMathExpressionSelector(Parser.parse(expression, this));
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExprEvalColumnSelector makeMathExpressionSelector(final Expr parsed)
    {
      final Map<String, Supplier> values = Maps.newHashMap();
      for (String columnName : Parser.findRequiredBindings(parsed)) {
        ObjectColumnSelector<Object> value = makeObjectColumnSelector(columnName);
        values.put(columnName, value == null ? ColumnSelectors.nullObjectSelector(ValueDesc.UNKNOWN) : value);
      }
      final Expr.NumericBinding binding = Parser.withSuppliers(values);
      return new ExprEvalColumnSelector()
      {
        @Override
        public ValueDesc typeOfObject()
        {
          return parsed.returns();
        }

        @Override
        public ExprEval get()
        {
          return parsed.eval(binding);
        }
      };
    }
  }

  abstract class ExprUnSupport extends Predicate
  {
    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      throw new UnsupportedOperationException("makeMathExpressionSelector");
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(Expr parsed)
    {
      throw new UnsupportedOperationException("makeMathExpressionSelector");
    }
  }
}
