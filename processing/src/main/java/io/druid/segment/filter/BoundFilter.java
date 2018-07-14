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

package io.druid.segment.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BitmapType;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.ExprEvalColumnSelector;
import io.druid.segment.column.BitmapIndex;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;

public class BoundFilter implements Filter
{
  private final BoundDimFilter boundDimFilter;
  private final ExtractionFn extractionFn;

  public BoundFilter(final BoundDimFilter boundDimFilter)
  {
    this.boundDimFilter = boundDimFilter;
    this.extractionFn = boundDimFilter.getExtractionFn();
  }

  @Override
  public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
  {
    if (extractionFn != null || boundDimFilter.getExpression() != null) {
      return null;
    }
    BitmapFactory factory = selector.getBitmapFactory();
    final int[] range = toRange(selector.getBitmapIndex(boundDimFilter.getDimension()));
    if (range == ALL) {
      return DimFilters.makeTrue(factory, selector.getNumRows());
    } else if (range == NONE) {
      return DimFilters.makeFalse(factory);
    }
    final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
    for (int i = range[0]; i < range[1]; i++) {
      bitmap.add(i);
    }
    return factory.makeImmutableBitmap(bitmap);
  }

  @Override
  public ImmutableBitmap getBitmapIndex(
      BitmapIndexSelector selector,
      EnumSet<BitmapType> using,
      ImmutableBitmap baseBitmap
  )
  {
    String dimension = boundDimFilter.getDimension();
    String expression = boundDimFilter.getExpression();
    if (expression != null || !boundDimFilter.isLexicographic() || extractionFn != null) {

      Predicate<String> predicate = toPredicate(ValueDesc.STRING);
      if (expression != null) {
        Expr expr = Parser.parse(expression);
        dimension = Iterables.getOnlyElement(Parser.findRequiredBindings(expr));
        predicate = Predicates.compose(predicate, Parser.asStringFunction(expr));
      }
      return Filters.matchPredicate(
          dimension,
          selector,
          predicate
      );
    }
    return toRangeBitmap(selector, dimension);
  }

  private static final int[] ALL = new int[] {0, Integer.MAX_VALUE};
  private static final int[] NONE = new int[] {0, 0};

  private int[] toRange(BitmapIndex bitmapIndex)
  {
    if (bitmapIndex == null || bitmapIndex.getCardinality() == 0) {
      return toPredicate(ValueDesc.STRING).apply(null) ? ALL : NONE;
    }

    // search for start, end indexes in the bitmaps; then include all bitmaps between those points

    final int startIndex; // inclusive
    final int endIndex; // exclusive

    if (!boundDimFilter.hasLowerBound()) {
      startIndex = 0;
    } else {
      final int found = bitmapIndex.getIndex(boundDimFilter.getLower());
      if (found >= 0) {
        startIndex = boundDimFilter.isLowerStrict() ? found + 1 : found;
      } else {
        startIndex = -(found + 1);
      }
    }

    if (!boundDimFilter.hasUpperBound()) {
      endIndex = bitmapIndex.getCardinality();
    } else {
      final int found = bitmapIndex.getIndex(boundDimFilter.getUpper());
      if (found >= 0) {
        endIndex = boundDimFilter.isUpperStrict() ? found : found + 1;
      } else {
        endIndex = -(found + 1);
      }
    }
    return new int[] {startIndex, endIndex};
  }

  private ImmutableBitmap toRangeBitmap(BitmapIndexSelector selector, String dimension)
  {
    final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
    final int[] range = toRange(bitmapIndex);

    final BitmapFactory factory = selector.getBitmapFactory();
    if (range == ALL) {
      return DimFilters.makeTrue(factory, selector.getNumRows());
    } else if (range == NONE) {
      return DimFilters.makeFalse(factory);
    }

    // search for start, end indexes in the bitmaps; then include all bitmaps between those points

    final int startIndex = range[0];
    final int endIndex = range[1];

    return factory.union(
        new Iterable<ImmutableBitmap>()
        {
          @Override
          public Iterator<ImmutableBitmap> iterator()
          {
            return new Iterator<ImmutableBitmap>()
            {
              int currIndex = startIndex;

              @Override
              public boolean hasNext()
              {
                return currIndex < endIndex;
              }

              @Override
              public ImmutableBitmap next()
              {
                return bitmapIndex.getBitmap(currIndex++);
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }
        }
    );
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    if (boundDimFilter.getDimension() != null) {
      ValueDesc type = factory.getColumnType(boundDimFilter.getDimension());
      if (type == null || type.isStringOrDimension() || ValueDesc.isMultiValued(type)) {
        type = ValueDesc.STRING;
      }
      return Filters.toValueMatcher(factory, boundDimFilter.getDimension(), toPredicate(type));
    }
    ExprEvalColumnSelector selector = factory.makeMathExpressionSelector(boundDimFilter.getExpression());
    return Filters.toValueMatcher(ColumnSelectors.asStringSelector(selector), toPredicate(selector.typeOfObject()));
  }

  private  <T> Predicate<T> toPredicate(ValueDesc valueDesc)
  {
    Preconditions.checkArgument(extractionFn == null || valueDesc.isStringOrDimension());

    final String lower = boundDimFilter.getLower();
    final String upper = boundDimFilter.getUpper();

    if (valueDesc.isStringOrDimension()) {
      return asPredicate(lower, upper, boundDimFilter.getComparator());
    }
    ValueType type = ValueDesc.assertPrimitive(valueDesc).type();
    return asPredicate(type.cast(lower), type.cast(upper), type.comparator());
  }

  private <T> Predicate<T> asPredicate(final Object lower, final Object upper, final Comparator comparator)
  {
    final boolean lowerNull = StringUtils.isNullOrEmpty(lower);
    final boolean upperNull = StringUtils.isNullOrEmpty(upper);

    return new Predicate<T>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public boolean apply(T input)
      {
        if (extractionFn != null) {
          input = (T) extractionFn.apply(input);
        }
        if (input == null) {
          return (!boundDimFilter.hasLowerBound()
                  || (lowerNull && !boundDimFilter.isLowerStrict())) // lower bound allows null
                 && (!boundDimFilter.hasUpperBound()
                     || !upperNull
                     || !boundDimFilter.isUpperStrict()) // upper bound allows null
              ;
        }
        int lowerComparing = 1;
        int upperComparing = 1;
        if (boundDimFilter.hasLowerBound()) {
          lowerComparing = comparator.compare(input, lower);
        }
        if (boundDimFilter.hasUpperBound()) {
          upperComparing = comparator.compare(upper, input);
        }
        if (boundDimFilter.isLowerStrict() && boundDimFilter.isUpperStrict()) {
          return ((lowerComparing > 0)) && (upperComparing > 0);
        } else if (boundDimFilter.isLowerStrict()) {
          return (lowerComparing > 0) && (upperComparing >= 0);
        } else if (boundDimFilter.isUpperStrict()) {
          return (lowerComparing >= 0) && (upperComparing > 0);
        }
        return (lowerComparing >= 0) && (upperComparing >= 0);
      }
    };
  }
}
