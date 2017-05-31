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

package io.druid.segment;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.GuavaUtils;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprType;
import io.druid.math.expr.Parser;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.ColumnCapabilities;

import java.util.Map;

/**
 * Factory class for MetricSelectors
 */
public interface ColumnSelectorFactory
{
  public Iterable<String> getColumnNames();
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec);
  public FloatColumnSelector makeFloatColumnSelector(String columnName);
  public DoubleColumnSelector makeDoubleColumnSelector(String columnName);
  public LongColumnSelector makeLongColumnSelector(String columnName);
  public ObjectColumnSelector makeObjectColumnSelector(String columnName);
  public ExprEvalColumnSelector makeMathExpressionSelector(String expression);
  public ColumnCapabilities getColumnCapabilities(String columnName);

  abstract class ExprSupport implements ColumnSelectorFactory
  {
    @Override
    @SuppressWarnings("unchecked")
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      final Expr parsed = Parser.parse(expression);
      final Map<String, DSuppliers.TypedSupplier> values = Maps.newHashMap();
      for (String columnName : Parser.findRequiredBindings(parsed)) {
        values.put(columnName, makeObjectColumnSelector(columnName));
      }
      final ExprType expected = parsed.type(Parser.withTypeSuppliers(values));
      final Expr.NumericBinding binding = Parser.withSuppliers(
          Maps.transformValues(
              values,
              GuavaUtils.<DSuppliers.TypedSupplier, Supplier>caster()
          )
      );
      return new ExprEvalColumnSelector()
      {
        @Override
        public ExprType typeOfObject()
        {
          return expected;
        }

        @Override
        public ExprEval get()
        {
          return parsed.eval(binding);
        }
      };
    }
  }

  abstract class ExprUnSupport implements ColumnSelectorFactory
  {
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      throw new UnsupportedOperationException("makeMathExpressionSelector");
    }
  }
}
