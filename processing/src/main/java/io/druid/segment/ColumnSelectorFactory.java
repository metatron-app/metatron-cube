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

import com.google.common.collect.Maps;
import io.druid.common.guava.DSuppliers.TypedSupplier;
import io.druid.common.guava.DSuppliers.WithRawAccess;
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
  Iterable<String> getColumnNames();

  default DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    DimensionSelector selector = VirtualColumns.toDimensionSelector(
        makeObjectColumnSelector(dimensionSpec.getDimension()),
        dimensionSpec.getExtractionFn()
    );
    return dimensionSpec.decorate(selector, this);
  }

  default FloatColumnSelector makeFloatColumnSelector(String columnName)
  {
    return ColumnSelectors.asFloat(makeObjectColumnSelector(columnName));
  }

  default DoubleColumnSelector makeDoubleColumnSelector(String columnName)
  {
    return ColumnSelectors.asDouble(makeObjectColumnSelector(columnName));
  }

  default LongColumnSelector makeLongColumnSelector(String columnName)
  {
    return ColumnSelectors.asLong(makeObjectColumnSelector(columnName));
  }

  ObjectColumnSelector makeObjectColumnSelector(String columnName);
  ExprEvalColumnSelector makeMathExpressionSelector(String expression);
  ExprEvalColumnSelector makeMathExpressionSelector(Expr expression);
  ValueMatcher makePredicateMatcher(DimFilter filter);

  default Map<String, String> getDescriptor(String columnName)
  {
    return null;
  }

  abstract class Predicate implements ColumnSelectorFactory
  {
    @Override
    public ValueMatcher makePredicateMatcher(DimFilter filter)
    {
      return filter.toFilter(this).makeMatcher(this);
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
    public ExprEvalColumnSelector makeMathExpressionSelector(final Expr parsed)
    {
      final Map<String, TypedSupplier> values = Maps.newHashMap();
      final Map<String, WithRawAccess> rawAccessible = Maps.newHashMap();
      for (String columnName : Parser.findRequiredBindings(parsed)) {
        ObjectColumnSelector value = makeObjectColumnSelector(columnName);
        values.put(columnName, value == null ? ColumnSelectors.nullObjectSelector(ValueDesc.UNKNOWN) : value);
        if (value instanceof WithRawAccess) {
          rawAccessible.put(columnName, (WithRawAccess) value);
        }
      }
      final Expr optimized = Parser.optimize(parsed, values, rawAccessible);
      final Expr.NumericBinding binding = Parser.withTypedSuppliers(values);
      return new ExprEvalColumnSelector()
      {
        @Override
        public Expr getExpression()
        {
          return optimized;
        }

        @Override
        public ExprEval get()
        {
          return optimized.eval(binding);
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
