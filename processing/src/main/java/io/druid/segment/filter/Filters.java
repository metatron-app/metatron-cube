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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.IntPredicate;
import io.druid.common.utils.Ranges;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Expression;
import io.druid.math.expr.Expression.AndExpression;
import io.druid.math.expr.Expression.FuncExpression;
import io.druid.math.expr.Expression.NotExpression;
import io.druid.math.expr.Expression.OrExpression;
import io.druid.math.expr.Expressions;
import io.druid.math.expr.Parser;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.MetricBitmap;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class Filters
{
  private static final Logger logger = new Logger(Filters.class);

  /**
   * Convert a list of DimFilters to a list of Filters.
   *
   * @param dimFilters list of DimFilters, should all be non-null
   *
   * @return list of Filters
   */
  public static List<Filter> toFilters(List<DimFilter> dimFilters)
  {
    return ImmutableList.copyOf(
        FunctionalIterable
            .create(dimFilters)
            .transform(
                new Function<DimFilter, Filter>()
                {
                  @Override
                  public Filter apply(DimFilter input)
                  {
                    return input.toFilter();
                  }
                }
            )
    );
  }

  /**
   * Convert a DimFilter to a Filter.
   *
   * @param dimFilter dimFilter
   *
   * @return converted filter, or null if input was null
   */
  public static Filter toFilter(DimFilter dimFilter)
  {
    return dimFilter == null ? null : dimFilter.optimize().toFilter();
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toValueMatcher(ColumnSelectorFactory factory, String column, Predicate predicate)
  {
    ValueDesc columnType = factory.getColumnType(column);
    if (columnType == null) {
      return BooleanValueMatcher.of(predicate.apply(null));
    }
    if (ValueDesc.isDimension(columnType)) {
      return Filters.toValueMatcher(factory.makeDimensionSelector(DefaultDimensionSpec.of(column)), predicate);
    }
    return Filters.toValueMatcher(ColumnSelectors.toDimensionalSelector(factory, column), predicate);
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toValueMatcher(
      final ObjectColumnSelector selector,
      final Predicate predicate
  )
  {
    if (ValueDesc.isPrimitive(selector.type())) {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          return predicate.apply(selector.get());
        }
      };
    }
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        Object object = selector.get();
        if (object == null || !object.getClass().isArray()) {
          return predicate.apply(Objects.toString(object, null));
        }
        int length = Array.getLength(object);
        for (int i = 0; i < length; i++) {
          if (predicate.apply(Objects.toString(Array.get(object, i), null))) {
            return true;
          }
        }
        return false;
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toValueMatcher(
      final DimensionSelector selector,
      final Predicate predicate
  )
  {
    // Check every value in the dimension, as a String.
    final int cardinality = selector.getValueCardinality();
    final BitSet valueIds = new BitSet(cardinality);
    for (int i = 0; i < cardinality; i++) {
      if (predicate.apply(selector.lookupName(i))) {
        valueIds.set(i);
      }
    }
    final boolean allowNull = predicate.apply(null);

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        final IndexedInts row = selector.getRow();
        final int length = row.size();
        if (length == 0) {
          return allowNull;
        } else if (length == 1) {
          return valueIds.get(row.get(0));
        }
        for (int i = 0; i < length; i++) {
          if (valueIds.get(row.get(i))) {
            return true;
          }
        }
        return false;
      }
    };
  }

  public static ValueMatcher toValueMatcher(
      final DimensionSelector selector,
      final IntPredicate predicate,
      final boolean allowNull
  )
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        final IndexedInts row = selector.getRow();
        final int length = row.size();
        if (length == 0) {
          return allowNull;
        }
        for (int i = 0; i < length; i++) {
          if (predicate.apply(row.get(i))) {
            return true;
          }
        }
        return false;
      }
    };
  }

  public static ImmutableBitmap matchPredicateValues(
      final String dimension,
      final BitmapIndexSelector selector,
      final Predicate<String> predicate
  )
  {
    Preconditions.checkNotNull(dimension, "dimension");
    Preconditions.checkNotNull(selector, "selector");
    Preconditions.checkNotNull(predicate, "predicate");

    // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
    final Indexed<String> dimValues = selector.getDimensionValues(dimension);
    if (dimValues == null || dimValues.size() == 0) {
      return selector.getBitmapFactory().makeEmptyImmutableBitmap();
    }

    final BitmapFactory factory = selector.getBitmapFactory();
    final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
    for (int i = 0; i < dimValues.size(); i++) {
      if (predicate.apply(dimValues.get(i))) {
        bitmap.add(i);
      }
    }
    return factory.makeImmutableBitmap(bitmap);
  }

  /**
   * Return the union of bitmaps for all values matching a particular predicate.
   *
   * @param dimension dimension to look at
   * @param selector  bitmap selector
   * @param predicate predicate to use
   *
   * @return bitmap of matching rows
   */
  public static ImmutableBitmap matchPredicate(
      final String dimension,
      final BitmapIndexSelector selector,
      final Predicate<String> predicate
  )
  {
    Preconditions.checkNotNull(dimension, "dimension");
    Preconditions.checkNotNull(selector, "selector");
    Preconditions.checkNotNull(predicate, "predicate");

    // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
    final Indexed<String> dimValues = selector.getDimensionValues(dimension);
    if (dimValues == null || dimValues.size() == 0) {
      if (predicate.apply(null)) {
        return selector.getBitmapFactory().complement(
            selector.getBitmapFactory().makeEmptyImmutableBitmap(),
            selector.getNumRows()
        );
      } else {
        return selector.getBitmapFactory().makeEmptyImmutableBitmap();
      }
    }

    // Apply predicate to all dimension values and union the matching bitmaps
    final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
    return selector.getBitmapFactory().union(
        new Iterable<ImmutableBitmap>()
        {
          @Override
          public Iterator<ImmutableBitmap> iterator()
          {
            return new Iterator<ImmutableBitmap>()
            {
              int currIndex = 0;

              @Override
              public boolean hasNext()
              {
                return currIndex < bitmapIndex.getCardinality();
              }

              @Override
              public ImmutableBitmap next()
              {
                while (currIndex < bitmapIndex.getCardinality() &&
                       !predicate.apply(dimValues.get(currIndex))) {
                  currIndex++;
                }

                if (currIndex == bitmapIndex.getCardinality()) {
                  return bitmapIndex.getBitmapFactory().makeEmptyImmutableBitmap();
                }

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

  public static DimFilter[] partitionWithBitmapSupport(DimFilter current, final RowResolver resolver)
  {
    current = Filters.convertToCNF(current);
    if (current == null) {
      return null;
    }
    return partitionFilterWith(
        current, new Predicate<DimFilter>()
        {
          @Override
          public boolean apply(DimFilter input)
          {
            return input.toFilter().supportsBitmap(resolver);
          }
        }
    );
  }

  public static Set<String> getDependents(DimFilter filter)
  {
    Set<String> handler = Sets.newHashSet();
    filter.addDependent(handler);
    return handler;
  }

  @SuppressWarnings("unchecked")
  public static ImmutableBitmap toBitmap(DimFilter filter, BitmapFactory factory, Map<String, MetricBitmap> metrics)
  {
    if (filter instanceof SelectorDimFilter) {
      SelectorDimFilter selector = (SelectorDimFilter) filter;
      MetricBitmap metricBitmap = metrics.get(selector.getDimension());
      if (metricBitmap != null) {
        return metricBitmap.filterFor(Range.closed(selector.getValue(), selector.getValue()));
      }
    } else if (filter instanceof InDimFilter) {
      InDimFilter selector = (InDimFilter) filter;
      MetricBitmap metricBitmap = metrics.get(selector.getDimension());
      if (metricBitmap != null) {
        List<ImmutableBitmap> bitmaps = Lists.newArrayList();
        for (String value : selector.getValues()) {
          ImmutableBitmap bitmap = metricBitmap.filterFor(Range.closed(value, value));
          if (bitmap == null) {
            return null;
          }
          bitmaps.add(bitmap);
        }
        return metricBitmap.getFactory().union(bitmaps);
      }
    } else if (filter instanceof MathExprFilter) {
      MathExprFilter selector = (MathExprFilter) filter;
      Expr expr = Parser.parse(selector.getExpression());
      Expr cnf = Expressions.convertToCNF(expr, Parser.EXPR_FACTORY);
      return toBitmap(cnf, factory, metrics, false);
    }
    return null;
  }

  public static ImmutableBitmap toBitmap(
      Expression tree,
      BitmapFactory factory,
      Map<String, MetricBitmap> metrics,
      boolean withNot
  )
  {
    if (tree instanceof FuncExpression) {
      List<String> required = Parser.findRequiredBindings((Expr) tree);
      if (required.size() != 1 || !metrics.containsKey(required.get(0))) {
        return null;
      }
      MetricBitmap metric = metrics.get(required.get(0));
      ImmutableBitmap bitmap = leafToRanges((FuncExpression) tree, metric, withNot);
      if (bitmap != null) {
        logger.debug("%s : %,d / %,d", tree, bitmap.size(), metric.rows());
      }
      return bitmap;
    } else if (tree instanceof AndExpression) {
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      for (Expression child : ((Expression.BooleanExpression)tree).getChildren()) {
        ImmutableBitmap extracted = toBitmap(child, factory, metrics, withNot);
        if (extracted != null) {
          bitmaps.add(extracted);
        }
      }
      return factory.intersection(bitmaps);
    } else if (tree instanceof OrExpression) {
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      for (Expression child : ((Expression.BooleanExpression)tree).getChildren()) {
        ImmutableBitmap extracted = toBitmap(child, factory, metrics, withNot);
        if (extracted == null) {
          return null;
        }
        bitmaps.add(extracted);
      }
      return factory.union(bitmaps);
    } else if (tree instanceof NotExpression) {
      return toBitmap(((NotExpression) tree).getChild(), factory, metrics, !withNot);
    }
    return null;
  }

  // constants need to be calculated apriori
  @SuppressWarnings("unchecked")
  private static ImmutableBitmap leafToRanges(
      FuncExpression expression,
      MetricBitmap metric,
      boolean withNot
  )
  {
    final ValueType type = metric.type();
    final BitmapFactory factory = metric.getFactory();

    final Comparable constant = getOnlyConstant(expression.getChildren(), type);
    switch (expression.op()) {
      case "<":
        return constant == null ? null :
               metric.filterFor(withNot ? Range.atLeast(constant) : Range.lessThan(constant));
      case ">":
        return constant == null ? null :
               metric.filterFor(withNot ? Range.atMost(constant) : Range.greaterThan(constant));
      case "<=":
        return constant == null ? null :
               metric.filterFor(withNot ? Range.greaterThan(constant) : Range.atMost(constant));
      case ">=":
        return constant == null ? null :
               metric.filterFor(withNot ? Range.lessThan(constant) : Range.atLeast(constant));
      case "==":
        if (constant == null) {
          return null;
        }
        if (withNot) {
          return factory.union(
              Arrays.asList(metric.filterFor(Range.lessThan(constant)), metric.filterFor(Range.greaterThan(constant)))
          );
        }
        return metric.filterFor(Range.closed(constant, constant));
      case "between":
        List<Comparable> constants = getConstants(expression.getChildren(), type);
        if (constants.size() != 2) {
          return null;
        }
        Comparable value1 = constants.get(0);
        Comparable value2 = constants.get(1);
        if (value1 == null || value2 == null) {
          return null;
        }
        if (value1.compareTo(value2) > 0) {
          Comparable x = value1;
          value1 = value2;
          value2 = x;
        }
        if (withNot) {
          return factory.union(
              Arrays.asList(metric.filterFor(Range.lessThan(value1)), metric.filterFor(Range.greaterThan(value2)))
          );
        }
        return metric.filterFor(Range.closed(value1, value2));
      case "in":
        if (withNot) {
          return null;  // hard to be expressed with bitmap
        }
        List<ImmutableBitmap> bitmaps = Lists.newArrayList();
        for (Comparable value : getConstants(expression.getChildren(), type)) {
          bitmaps.add(metric.filterFor(Range.closed(value, value)));
        }
        return factory.union(bitmaps);
    }
    return null;
  }

  private static Comparable getOnlyConstant(List<Expression> children, ValueType type)
  {
    Comparable constant = null;
    for (Expression expr : children) {
      if (expr instanceof Expression.ConstExpression) {
        if (constant != null) {
          return null;
        }
        constant = type.cast(((Expression.ConstExpression) expr).get());
      }
    }
    return constant;
  }

  private static List<Comparable> getConstants(List<Expression> children, ValueType type)
  {
    List<Comparable> constants = Lists.newArrayList();
    for (Expression expr : children) {
      if (expr instanceof Expression.ConstExpression) {
        constants.add(type.cast(((Expression.ConstExpression)expr).get()));
      }
    }
    return constants;
  }

  private static DimFilter[] partitionFilterWith(DimFilter current, Predicate<DimFilter> predicate)
  {
    if (current == null) {
      return null;
    }
    List<DimFilter> bitmapIndexSupported = Lists.newArrayList();
    List<DimFilter> bitmapIndexNotSupported = Lists.newArrayList();

    traverse(current, predicate, bitmapIndexSupported, bitmapIndexNotSupported);

    return new DimFilter[]{andFilter(bitmapIndexSupported), andFilter(bitmapIndexNotSupported)};
  }

  private static void traverse(
      DimFilter current,
      Predicate<DimFilter> predicate,
      List<DimFilter> support,
      List<DimFilter> notSupport
  )
  {
    if (current instanceof AndDimFilter) {
      for (DimFilter child : ((AndDimFilter) current).getChildren()) {
        traverse(child, predicate, support, notSupport);
      }
    } else {
      if (predicate.apply(current)) {
        support.add(current);
      } else {
        notSupport.add(current);
      }
    }
  }

  private static DimFilter andFilter(List<DimFilter> filters)
  {
    return filters.isEmpty() ? null : filters.size() == 1 ? filters.get(0) : new AndDimFilter(filters);
  }

  public static DimFilter convertToCNF(DimFilter current)
  {
    return current == null ? null : Expressions.convertToCNF(current.optimize(), new DimFilter.Factory());
  }

  public static Filter convertToCNF(Filter current)
  {
    return Expressions.convertToCNF(current, new Filter.Factory());
  }

  // should be string type
  public static DimFilter toFilter(String dimension, List<Range> ranges)
  {
    Iterable<Range> filtered = Iterables.filter(ranges, Ranges.VALID);
    List<String> equalValues = Lists.newArrayList();
    List<DimFilter> dimFilters = Lists.newArrayList();
    for (Range range : filtered) {
      String lower = range.hasLowerBound() ? (String) range.lowerEndpoint() : null;
      String upper = range.hasUpperBound() ? (String) range.upperEndpoint() : null;
      if (lower == null && upper == null) {
        return null;
      }
      if (Objects.equals(lower, upper)) {
        equalValues.add(lower);
        continue;
      }
      boolean lowerStrict = range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN;
      boolean upperStrict = range.hasUpperBound() && range.upperBoundType() == BoundType.OPEN;
      dimFilters.add(new BoundDimFilter(dimension, lower, upper, lowerStrict, upperStrict, false, null));
    }
    if (equalValues.size() > 1) {
      dimFilters.add(new InDimFilter(dimension, equalValues, null));
    } else if (equalValues.size() == 1) {
      dimFilters.add(new SelectorDimFilter(dimension, equalValues.get(0), null));
    }
    DimFilter dimFilter = new OrDimFilter(dimFilters).optimize();
    logger.info("Converted dimension '%s' ranges %s to filter %s", dimension, ranges, dimFilter);
    return dimFilter;
  }
}
