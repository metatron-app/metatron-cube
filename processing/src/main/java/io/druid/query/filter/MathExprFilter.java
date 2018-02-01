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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.common.StringUtils;
import io.druid.common.guava.DSuppliers;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Expr.NumericBinding;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ExprEvalColumnSelector;
import io.druid.segment.column.BitmapIndex;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class MathExprFilter implements DimFilter
{
  private final String expression;

  @JsonCreator
  public MathExprFilter(
      @JsonProperty("expression") String expression
  )
  {
    this.expression = Preconditions.checkNotNull(expression, "expression can not be null");
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] expressionBytes = StringUtils.toUtf8(expression);
    return ByteBuffer.allocate(1 + expressionBytes.length)
                     .put(DimFilterCacheHelper.MATH_EXPR_CACHE_ID)
                     .put(expressionBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    return this;
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.addAll(Parser.findRequiredBindings(expression));
  }

  @Override
  public Filter toFilter()
  {
    return new Filter()
    {
      @Override
      public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
      {
        final Expr expr = Parser.parse(expression);
        String dimension = Iterables.getOnlyElement(Parser.findRequiredBindings(expr));
        BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);

        BitmapFactory factory = selector.getBitmapFactory();

        final int cardinality = bitmapIndex.getCardinality();
        final DSuppliers.HandOver<String> handOver = new DSuppliers.HandOver<>();
        final NumericBinding binding = Parser.withSuppliers(ImmutableMap.<String, Supplier>of(dimension, handOver));

        final MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
        for (int i = 0; i < cardinality; i++) {
          handOver.set(bitmapIndex.getValue(i));
          if (expr.eval(binding).asBoolean()) {
            mutableBitmap.add(i);
          }
        }
        handOver.set(null);
        return factory.makeImmutableBitmap(mutableBitmap);
      }

      @Override
      public ImmutableBitmap getBitmapIndex(
          BitmapIndexSelector selector,
          EnumSet<BitmapType> using,
          ImmutableBitmap baseBitmap
      )
      {
        final Expr expr = Parser.parse(expression);
        final String dimension = Iterables.getOnlyElement(Parser.findRequiredBindings(expr));

        final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
        final int cardinality = bitmapIndex.getCardinality();
        final DSuppliers.HandOver<String> handOver = new DSuppliers.HandOver<>();
        final NumericBinding binding = Parser.withSuppliers(ImmutableMap.<String, Supplier>of(dimension, handOver));

        final List<ImmutableBitmap> bitmaps = Lists.newArrayList();
        for (int i = 0; i < cardinality; i++) {
          handOver.set(bitmapIndex.getValue(i));
          if (expr.eval(binding).asBoolean()) {
            bitmaps.add(bitmapIndex.getBitmap(i));
          }
        }
        handOver.set(null);
        return selector.getBitmapFactory().union(bitmaps);
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
      {
        final ExprEvalColumnSelector selector = columnSelectorFactory.makeMathExpressionSelector(expression);
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            return selector.get().asBoolean();
          }
        };
      }

      @Override
      public String toString()
      {
        return MathExprFilter.this.toString();
      }
    };
  }

  @Override
  public String toString()
  {
    return "MathExprFilter{" +
           "expression='" + expression + '\'' +
           '}';
  }

  @Override
  public int hashCode()
  {
    return expression.hashCode();
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

    MathExprFilter that = (MathExprFilter) o;

    if (!expression.equals(that.expression)) {
      return false;
    }

    return true;
  }
}
