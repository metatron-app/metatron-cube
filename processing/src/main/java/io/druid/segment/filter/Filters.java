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

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.cache.Cache;
import io.druid.common.Cacheable;
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.IntPredicate;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Expression;
import io.druid.math.expr.Expression.AndExpression;
import io.druid.math.expr.Expression.FuncExpression;
import io.druid.math.expr.Expression.NotExpression;
import io.druid.math.expr.Expression.OrExpression;
import io.druid.math.expr.Expression.RelationExpression;
import io.druid.math.expr.Expressions;
import io.druid.math.expr.Parser;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilter.BooleanColumnSupport;
import io.druid.query.filter.DimFilter.MathcherOnly;
import io.druid.query.filter.DimFilter.RangeFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ExprEvalColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.bitmap.BitSetBitmapFactory;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.column.SecondaryIndex;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.lucene.Lucenes;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.apache.commons.io.IOUtils;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 */
public class Filters
{
  private static final Logger logger = new Logger(Filters.class);

  public static Filter and(Filter... filters)
  {
    return and(Arrays.asList(filters));
  }

  public static Filter and(List<Filter> filters)
  {
    List<Filter> list = filterNull(filters);
    return list.isEmpty() ? null : list.size() == 1 ? list.get(0) : new AndFilter(list);
  }

  public static Filter or(Filter... filters)
  {
    return or(Arrays.asList(filters));
  }

  public static Filter or(List<Filter> filters)
  {
    List<Filter> list = filterNull(filters);
    return list.isEmpty() ? null : list.size() == 1 ? list.get(0) : new OrFilter(list);
  }

  public static <T> List<T> filterNull(Iterable<T> filters)
  {
    return Lists.newArrayList(Iterables.filter(filters, Predicates.notNull()));
  }

  /**
   * Convert a list of DimFilters to a list of Filters.
   *
   * @param dimFilters list of DimFilters, should all be non-null
   * @param resolver
   *
   * @return list of Filters
   */
  public static List<Filter> toFilters(List<DimFilter> dimFilters, final TypeResolver resolver)
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
                    return input.toFilter(resolver);
                  }
                }
            )
    );
  }

  /**
   * Convert a DimFilter to a Filter.
   *
   * @param dimFilter dimFilter
   * @param resolver
   *
   * @return converted filter, or null if input was null
   */
  public static Filter toFilter(DimFilter dimFilter, TypeResolver resolver)
  {
    return dimFilter == null ? null : dimFilter.toFilter(resolver);
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toValueMatcher(ColumnSelectorFactory factory, String column, Predicate predicate)
  {
    ValueDesc columnType = factory.resolve(column);
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
    final ValueDesc type = selector.type();
    if (ValueDesc.isPrimitive(type)) {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          return predicate.apply(selector.get());
        }
      };
    }
    if (ValueDesc.isArray(type) || ValueDesc.isMultiValued(type)) {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final Object object = selector.get();
          if (object == null || !object.getClass().isArray()) {
            return predicate.apply(object);
          }
          final int length = Array.getLength(object);
          for (int i = 0; i < length; i++) {
            if (predicate.apply(Array.get(object, i))) {
              return true;
            }
          }
          return false;
        }
      };
    }
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return predicate.apply(selector.get());
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toValueMatcher(
      final DimensionSelector selector,
      final Predicate predicate
  )
  {
    final boolean allowNull = predicate.apply(null);
    // Check every value in the dimension, as a String.
    final int cardinality = selector.getValueCardinality();
    if (cardinality < 0) {
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
            return predicate.apply(selector.lookupName(row.get(0)));
          }
          for (int i = 0; i < length; i++) {
            if (predicate.apply(selector.lookupName(row.get(i)))) {
              return true;
            }
          }
          return false;
        }
      };
    }

    final BitSet valueIds = new BitSet(cardinality);
    for (int i = 0; i < cardinality; i++) {
      if (predicate.apply(selector.lookupName(i))) {
        valueIds.set(i);
      }
    }
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
      final Predicate<String> predicate,
      final FilterContext context
  )
  {
    final BitmapIndexSelector selector = context.indexSelector();
    Preconditions.checkNotNull(dimension, "dimension");
    Preconditions.checkNotNull(selector, "selector");
    Preconditions.checkNotNull(predicate, "predicate");

    // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
    final Indexed<String> dimValues = selector.getDimensionValues(dimension);
    if (dimValues == null || dimValues.size() == 0) {
      if (predicate.apply(null)) {
        return DimFilters.makeTrue(selector.getBitmapFactory(), selector.getNumRows());
      } else {
        return DimFilters.makeFalse(selector.getBitmapFactory());
      }
    }

    // Apply predicate to all dimension values and union the matching bitmaps
    final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
    return DimFilters.union(
        selector.getBitmapFactory(),
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

  public static Set<String> getDependents(DimFilter filter)
  {
    Set<String> handler = Sets.newHashSet();
    if (filter != null) {
      filter.addDependent(handler);
    }
    return handler;
  }

  private static final BitmapFactory[] BITMAP_FACTORIES = new BitmapFactory[]{
      new BitSetBitmapFactory(), new ConciseBitmapFactory(), new RoaringBitmapFactory()
  };

  public static FilterContext createFilterContext(
      final BitmapIndexSelector selector,
      final Cache cache,
      final String segmentId
  )
  {
    final byte bitmapCode = codeOfFactory(selector.getBitmapFactory());
    if (cache == null || cache == Cache.NULL || segmentId == null || bitmapCode >= BITMAP_FACTORIES.length) {
      return new FilterContext(selector);
    }
    final byte[] namespace = StringUtils.toUtf8(segmentId);
    return new FilterContext(selector)
    {
      @Override
      public BitmapHolder createBitmap(Cacheable filter, Supplier<BitmapHolder> populator)
      {
        final Cache.NamedKey key = new Cache.NamedKey(namespace, filter.getCacheKey());
        final byte[] cached = cache.get(key);
        if (cached != null) {
          ByteBuffer wrapped = ByteBuffer.wrap(cached);
          byte code = wrapped.get();
          boolean exact = wrapped.get() != 0;
          if (code == bitmapCode) {
            return BitmapHolder.of(exact, factory.mapImmutableBitmap(wrapped));
          }
          MutableBitmap mutable = factory.makeEmptyMutableBitmap();
          IntIterator iterators = BITMAP_FACTORIES[code].mapImmutableBitmap(wrapped).iterator();
          while (iterators.hasNext()) {
            mutable.add(iterators.next());
          }
          return BitmapHolder.of(exact, factory.makeImmutableBitmap(mutable));
        }
        final BitmapHolder holder = populator.get();
        if (holder != null && holder.size() < targetNumRows()) {
          byte exact = holder.exact() ? (byte) 0x01 : 0x00;
          cache.put(key, StringUtils.concat(new byte[]{bitmapCode, exact}, holder.bitmap().toBytes()));
        }
        return holder;
      }
    };
  }

  private static byte codeOfFactory(BitmapFactory factory)
  {
    if (factory instanceof BitSetBitmapFactory) {
      return 0x00;
    } else if (factory instanceof ConciseBitmapFactory) {
      return 0x01;
    } else if (factory instanceof RoaringBitmapFactory) {
      return 0x02;
    }
    return 0x7f;
  }

  public static BitmapHolder toBitmapHolder(DimFilter filter, FilterContext context)
  {
    long start = System.currentTimeMillis();
    BitmapHolder baseBitmap = toBitmapHolderRecurse(filter, context);
    if (baseBitmap != null && logger.isDebugEnabled()) {
      long elapsed = System.currentTimeMillis() - start;
      logger.debug("%s : %,d / %,d (%,d msec)", filter, baseBitmap.rhs.size(), context.numRows(), elapsed);
    }
    return baseBitmap;
  }

  private static BitmapHolder toBitmapHolderRecurse(DimFilter dimFilter, FilterContext context)
  {
    if (dimFilter instanceof RelationExpression) {
      List<BitmapHolder> holders = Lists.newArrayList();
      for (Expression child : ((RelationExpression) dimFilter).getChildren()) {
        BitmapHolder holder = toBitmapHolderRecurse((DimFilter) child, context);
        if (holder == null) {
          return null;
        }
        holders.add(holder);
      }
      // concise returns 1,040,187,360. roaring returns 0. makes wrong result anyway
      if (dimFilter instanceof AndExpression) {
        return BitmapHolder.intersection(context.factory, holders);
      } else if (dimFilter instanceof OrExpression) {
        return BitmapHolder.union(context.factory, holders);
      } else if (dimFilter instanceof NotExpression) {
        return BitmapHolder.not(context.factory, Iterables.getOnlyElement(holders), context.numRows());
      }
      return null;
    } else {
      return context.createBitmap(dimFilter);
    }
  }

  @SuppressWarnings("unchecked")
  static BitmapHolder leafToBitmap(DimFilter filter, FilterContext context)
  {
    Preconditions.checkArgument(!(filter instanceof RelationExpression));
    if (filter instanceof MathcherOnly) {
      return null;
    }
    final BitmapIndexSelector selector = context.selector;
    if (filter instanceof DimFilter.BestEffort) {
      return filter.toFilter(selector).getBitmapIndex(context);
    }
    if (filter instanceof MathExprFilter) {
      Expr expr = Parser.parse(((MathExprFilter) filter).getExpression(), selector);
      Expr cnf = Expressions.convertToCNF(expr, Parser.EXPR_FACTORY);
      return toExprBitmap(cnf, context, false);
    }
    String columnName = Iterables.getOnlyElement(Filters.getDependents(filter), null);
    if (columnName == null) {
      return null;
    }
    Column column = selector.getColumn(columnName);
    if (column == null) {
      return null;
    }
    ColumnCapabilities capabilities = column.getCapabilities();
    if (capabilities.hasBitmapIndexes()) {
      BitmapHolder holder = filter.toFilter(selector).getBitmapIndex(context);
      if (holder != null) {
        return holder;
      }
    }
    if (capabilities.getType() == ValueType.BOOLEAN && filter instanceof BooleanColumnSupport) {
      ImmutableBitmap bitmap = ((BooleanColumnSupport) filter).toBooleanFilter(selector, selector);
      if (bitmap != null) {
        return BitmapHolder.exact(bitmap);
      }
    }
    if (filter instanceof RangeFilter && ((RangeFilter) filter).possible(selector)) {
      SecondaryIndex.WithRange index = getWhatever(selector, columnName, SecondaryIndex.WithRange.class);
      if (index != null) {
        List<BitmapHolder> holders = Lists.newArrayList();
        for (Range range : ((RangeFilter) filter).toRanges(selector)) {
          holders.add(index.filterFor(range, context));
        }
        return BitmapHolder.intersection(context.factory, holders);
      }
      if (column.getType().isPrimitiveNumeric()) {
        ImmutableBitmap bitmap = null;
        if (filter instanceof BoundDimFilter) {
          Range range = Iterables.getOnlyElement(((BoundDimFilter) filter).toRanges(selector));
          bitmap = scanForRange(column.getGenericColumn(), range, context);
        } else if (filter instanceof SelectorDimFilter) {
          String value = ((SelectorDimFilter) filter).getValue();
          bitmap = scanForEqui(column.getGenericColumn(), value, context);
        } else if (filter instanceof InDimFilter) {
          Set<String> values = ((InDimFilter) filter).getValues();
          bitmap = scanForEqui(column.getGenericColumn(), values, context);
        }
        if (bitmap != null) {
          return BitmapHolder.exact(bitmap);
        }
      }
    }
    return null;
  }

  private static ImmutableBitmap scanForRange(GenericColumn column, Range range, FilterContext context)
  {
    final IntIterator iterator = context.getBaseBitmap() == null ? null : context.getBaseBitmap().iterator();
    final BitmapFactory factory = context.indexSelector().getBitmapFactory();
    try {
      final BoundType lowerType = range.hasLowerBound() ? range.lowerBoundType() : null;
      final BoundType upperType = range.hasUpperBound() ? range.upperBoundType() : null;
      if (column instanceof GenericColumn.FloatType) {
        final float lower = range.hasLowerBound() ? (Float) range.lowerEndpoint() : 0;
        final float upper = range.hasUpperBound() ? (Float) range.upperEndpoint() : 0;
        return ((GenericColumn.FloatType) column).collect(
            factory, iterator,
            range.hasLowerBound() && range.hasUpperBound() ?
            lowerType == BoundType.OPEN ?
            upperType == BoundType.OPEN ? f -> lower < f && f < upper : f -> lower < f && f <= upper :
            upperType == BoundType.OPEN ? f -> lower <= f && f < upper : f -> lower <= f && f <= upper :
            range.hasLowerBound() ? lowerType == BoundType.OPEN ? f -> lower < f : f -> lower <= f :
            upperType == BoundType.OPEN ? f -> f < upper : f -> f <= upper
        );
      } else if (column instanceof GenericColumn.DoubleType) {
        final double lower = range.hasLowerBound() ? (Double) range.lowerEndpoint() : 0;
        final double upper = range.hasUpperBound() ? (Double) range.upperEndpoint() : 0;
        return ((GenericColumn.DoubleType) column).collect(
            factory, iterator,
            range.hasLowerBound() && range.hasUpperBound() ?
            lowerType == BoundType.OPEN ?
            upperType == BoundType.OPEN ? d -> lower < d && d < upper : d -> lower < d && d <= upper :
            upperType == BoundType.OPEN ? d -> lower <= d && d < upper : d -> lower <= d && d <= upper :
            range.hasLowerBound() ? lowerType == BoundType.OPEN ? d -> lower < d : d -> lower <= d :
            upperType == BoundType.OPEN ? d -> d < upper : d -> d <= upper
        );
      } else if (column instanceof GenericColumn.LongType) {
        final long lower = range.hasLowerBound() ? (Long) range.lowerEndpoint() : 0;
        final long upper = range.hasUpperBound() ? (Long) range.upperEndpoint() : 0;
        return ((GenericColumn.LongType) column).collect(
            factory, iterator,
            range.hasLowerBound() && range.hasUpperBound() ?
            lowerType == BoundType.OPEN ?
            upperType == BoundType.OPEN ? l -> lower < l && l < upper : l -> lower < l && l <= upper :
            upperType == BoundType.OPEN ? l -> lower <= l && l < upper : l -> lower <= l && l <= upper :
            range.hasLowerBound() ? lowerType == BoundType.OPEN ? l -> lower < l : l -> lower <= l :
            upperType == BoundType.OPEN ? l -> l < upper : l -> l <= upper
        );
      }
    }
    catch (Exception e) {
      // ignore
    }
    finally {
      IOUtils.closeQuietly(column);
    }
    return null;
  }

  private static ImmutableBitmap scanForEqui(GenericColumn column, String value, FilterContext context)
  {
    final IntIterator iterator = context.getBaseBitmap() == null ? null : context.getBaseBitmap().iterator();
    final BitmapFactory factory = context.indexSelector().getBitmapFactory();
    try {
      if (StringUtils.isNullOrEmpty(value)) {
        return column.getNulls();
      } else if (column instanceof GenericColumn.FloatType) {
        final float fv = Rows.parseFloat(value);
        return ((GenericColumn.FloatType) column).collect(factory, iterator, f -> f == fv);
      } else if (column instanceof GenericColumn.DoubleType) {
        final double dv = Rows.parseDouble(value);
        return ((GenericColumn.DoubleType) column).collect(factory, iterator, d -> d == dv);
      } else if (column instanceof GenericColumn.LongType) {
        final long lv = Rows.parseLong(value);
        return ((GenericColumn.LongType) column).collect(factory, iterator, l -> l == lv);
      }
    }
    catch (Exception e) {
      // ignore
    }
    finally {
      IOUtils.closeQuietly(column);
    }
    return null;
  }

  private static ImmutableBitmap scanForEqui(GenericColumn column, Set<String> values, FilterContext context)
  {
    final IntIterator iterator = context.getBaseBitmap() == null ? null : context.getBaseBitmap().iterator();
    final BitmapFactory factory = context.indexSelector().getBitmapFactory();
    try {
      Iterable<String> filtered = values;
      ImmutableBitmap nulls = factory.makeEmptyImmutableBitmap();
      if (values.contains("")) {
        filtered = Iterables.filter(values, v -> !v.isEmpty());
        nulls = column.getNulls();
      }
      ImmutableBitmap collected = null;
      if (column instanceof GenericColumn.FloatType) {
        final FloatSet fset = new FloatOpenHashSet();
        for (String value : filtered) {
          fset.add(Rows.parseFloat(value).floatValue());
        }
        collected = ((GenericColumn.FloatType) column).collect(factory, iterator, f -> fset.contains(f));
      } else if (column instanceof GenericColumn.DoubleType) {
        final DoubleSet dset = new DoubleOpenHashSet();
        for (String value : filtered) {
          dset.add(Rows.parseDouble(value).doubleValue());
        }
        collected = ((GenericColumn.DoubleType) column).collect(factory, iterator, d -> dset.contains(d));
      } else if (column instanceof GenericColumn.LongType) {
        final LongSet lset = new LongOpenHashSet();
        for (String value : filtered) {
          lset.add(Rows.parseLong(value).longValue());
        }
        collected = ((GenericColumn.LongType) column).collect(factory, iterator, l -> lset.contains(l));
      }
      if (collected != null && !nulls.isEmpty()) {
        collected = factory.union(Arrays.asList(collected, nulls));
      }
      return collected;
    }
    catch (Exception e) {
      // ignore
    }
    finally {
      IOUtils.closeQuietly(column);
    }
    return null;
  }

  private static BitmapHolder toExprBitmap(Expression expr, FilterContext context, boolean withNot)
  {
    if (expr instanceof AndExpression) {
      List<BitmapHolder> holders = Lists.newArrayList();
      for (Expression child : ((AndExpression) expr).getChildren()) {
        BitmapHolder extracted = toExprBitmap(child, context, withNot);
        if (extracted != null) {
          holders.add(extracted);
        }
      }
      return BitmapHolder.intersection(context.factory, holders);
    } else if (expr instanceof OrExpression) {
      List<BitmapHolder> holders = Lists.newArrayList();
      for (Expression child : ((OrExpression) expr).getChildren()) {
        BitmapHolder extracted = toExprBitmap(child, context, withNot);
        if (extracted == null) {
          return null;
        }
        holders.add(extracted);
      }
      return BitmapHolder.union(context.factory, holders);
    } else if (expr instanceof NotExpression) {
      return toExprBitmap(((NotExpression) expr).getChild(), context, !withNot);
    } else {
      List<String> required = Parser.findRequiredBindings((Expr) expr);
      if (required.size() != 1) {
        return null;
      }
      String columnName = required.get(0);
      BitmapIndexSelector selector = context.selector;
      Column column = selector.getColumn(columnName);
      if (column == null) {
        return null;
      }
      ColumnCapabilities capabilities = column.getCapabilities();
      if (Evals.isLeafFunction((Expr) expr, columnName)) {
        FuncExpression funcExpr = (FuncExpression) expr;
        if (capabilities.hasBitmapIndexes()) {
          SecondaryIndex index = asSecondaryIndex(selector, columnName);
          BitmapHolder holder = leafToRanges(columnName, funcExpr, context, index, withNot);
          if (holder != null) {
            return holder;
          }
        }
        SecondaryIndex index = getWhatever(selector, columnName, SecondaryIndex.class);
        if (index != null) {
          long start = System.currentTimeMillis();
          BitmapHolder holder = leafToRanges(columnName, funcExpr, context, index, withNot);
          if (holder != null) {
            if (logger.isDebugEnabled()) {
              long elapsed = System.currentTimeMillis() - start;
              logger.debug(
                  "%s%s : %,d / %,d (%,d msec)", withNot ? "!" : "", expr, holder.size(), index.numRows(), elapsed
              );
            }
            return holder;
          }
        }
        // can be null for complex column
        GenericColumn generic = column.getGenericColumn();
        if (generic != null) {
          BitmapHolder holder = leafToRanges(columnName, funcExpr, context, asSecondaryIndex(generic), withNot);
          if (holder != null) {
            return holder;
          }
        }
      }
      if (capabilities.hasBitmapIndexes() || capabilities.getType() == ValueType.BOOLEAN) {
        // traverse all possible values
        BitmapHolder holder = ofExpr((Expr) expr).getBitmapIndex(context);
        if (holder != null && withNot) {
          holder = BitmapHolder.not(context.factory, holder, context.numRows());
        }
        return holder;
      }
      return null;
    }
  }

  public static Filter ofExpr(final Expr expr)
  {
    return new Filter()
    {
      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        final String dimension = Iterables.getOnlyElement(Parser.findRequiredBindings(expr), null);
        if (dimension == null) {
          return null;
        }
        final DSuppliers.HandOver<Object> handOver = new DSuppliers.HandOver<>();
        final Expr.NumericBinding binding = Parser.withSuppliers(ImmutableMap.<String, Supplier>of(dimension, handOver));

        final BitmapIndexSelector selector = context.indexSelector();
        final ColumnCapabilities capabilities = selector.getCapabilities(dimension);
        if (capabilities == null) {
          return null;
        }
        final List<ImmutableBitmap> bitmaps = Lists.newArrayList();
        final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
        if (bitmapIndex != null) {
          final int cardinality = bitmapIndex.getCardinality();
          for (int i = 0; i < cardinality; i++) {
            handOver.set(bitmapIndex.getValue(i));
            if (expr.eval(binding).asBoolean()) {
              bitmaps.add(bitmapIndex.getBitmap(i));
            }
          }
        } else if (capabilities.getType() == ValueType.BOOLEAN) {
          for (Boolean bool : new Boolean[]{null, true, false}) {
            handOver.set(bool);
            if (expr.eval(binding).asBoolean()) {
              bitmaps.add(selector.getBitmapIndex(dimension, bool));
            }
          }
        } else {
          return null;
        }

        return BitmapHolder.exact(DimFilters.union(selector.getBitmapFactory(), bitmaps));
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
      {
        return new ValueMatcher()
        {
          private final ExprEvalColumnSelector selector = factory.makeMathExpressionSelector(expr);

          @Override
          public boolean matches()
          {
            return selector.get().asBoolean();
          }
        };
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <T extends SecondaryIndex> T getWhatever(BitmapIndexSelector bitmaps, String column, Class<T> type)
  {
    SecondaryIndex bitmap = bitmaps.getBitSlicedBitmap(column);
    if (!type.isInstance(bitmap)) {
      bitmap = bitmaps.getLuceneIndex(column);
    }
    if (!type.isInstance(bitmap)) {
      bitmap = bitmaps.getMetricBitmap(column);
    }
    return type.isInstance(bitmap) ? (T) bitmap : null;
  }

  // constants need to be calculated apriori
  @SuppressWarnings("unchecked")
  private static BitmapHolder leafToRanges(
      String column,
      FuncExpression expression,
      FilterContext context,
      SecondaryIndex metric,
      boolean withNot
  )
  {
    final ValueType type = ValueDesc.assertPrimitive(metric.type()).type();

    final BitmapFactory factory = context.factory;
    if (Expressions.isCompare(expression.op())) {
      final Comparable constant = getOnlyConstant(expression.getChildren(), type);
      if (constant == null) {
        return null;
      }
      switch (expression.op()) {
        case "<":
          return metric instanceof SecondaryIndex.WithRange ? metric.filterFor(
              withNot ? Range.atLeast(constant) : Range.lessThan(constant), context) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.atLeast(column, constant) : Lucenes.lessThan(column, constant), context
                 ) :
                 null;
        case ">":
          return metric instanceof SecondaryIndex.WithRange ? metric.filterFor(
              withNot ? Range.atMost(constant) : Range.greaterThan(constant), context) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.atMost(column, constant) : Lucenes.greaterThan(column, constant), context
                 ) :
                 null;
        case "<=":
          return metric instanceof SecondaryIndex.WithRange ? metric.filterFor(
              withNot ? Range.greaterThan(constant) : Range.atMost(constant), context) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.greaterThan(column, constant) : Lucenes.atMost(column, constant), context
                 ) :
                 null;
        case ">=":
          return metric instanceof SecondaryIndex.WithRange ? metric.filterFor(
              withNot ? Range.lessThan(constant) : Range.atLeast(constant), context) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.lessThan(column, constant) : Lucenes.atLeast(column, constant), context
                 ) :
                 null;
        case "==":
          if (withNot) {
            return BitmapHolder.union(
                factory,
                Arrays.asList(
                    metric instanceof SecondaryIndex.WithRange ?
                    metric.filterFor(Range.lessThan(constant), context) :
                    metric instanceof LuceneIndex ?
                    metric.filterFor(Lucenes.lessThan(column, constant), context) :
                    null,
                    metric instanceof SecondaryIndex.WithRange ?
                    metric.filterFor(Range.greaterThan(constant), context) :
                    metric instanceof LuceneIndex ?
                    metric.filterFor(Lucenes.greaterThan(column, constant), context) :
                    null
                )
            );
          }
          return metric instanceof SecondaryIndex.WithRange ?
                 metric.filterFor(Range.closed(constant, constant), context) :
                 metric instanceof LuceneIndex ?
                 metric.filterFor(Lucenes.point(column, constant), context) :
                 null;
      }
    }

    switch (expression.op().toLowerCase()) {
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
          return BitmapHolder.union(
              factory,
              Arrays.asList(
                  metric instanceof SecondaryIndex.WithRange ?
                  metric.filterFor(Range.lessThan(value1), context) :
                  metric instanceof LuceneIndex ?
                  metric.filterFor(Lucenes.lessThan(column, value1), context) :
                  null,
                  metric instanceof SecondaryIndex.WithRange ?
                  metric.filterFor(Range.greaterThan(value2), context) :
                  metric instanceof LuceneIndex ? metric.filterFor(Lucenes.greaterThan(column, value2), context) :
                  null
              )
          );
        }
        return metric instanceof SecondaryIndex.WithRange ?
               metric.filterFor(Range.closed(value1, value2), context) :
               metric instanceof LuceneIndex ?
               metric.filterFor(Lucenes.closed(column, value1, value2), context) :
               null;
      case "in":
        if (withNot) {
          return null;  // hard to be expressed with bitmap
        }
        List<BitmapHolder> holders = Lists.newArrayList();
        for (Comparable value : getConstants(expression.getChildren(), type)) {
          holders.add(
              metric instanceof SecondaryIndex.WithRange ?
              metric.filterFor(Range.closed(value, value), context) :
              metric instanceof LuceneIndex ?
              metric.filterFor(Lucenes.point(column, value), context) :
              null
          );
        }
        return BitmapHolder.union(factory, holders);
      case "isnull":
        if (metric instanceof SecondaryIndex.SupportNull) {
          ImmutableBitmap bitmap = ((SecondaryIndex.SupportNull) metric).getNulls(context.baseBitmap);
          if (withNot) {
            bitmap = DimFilters.complement(context.factory, bitmap, context.numRows());
          }
          return BitmapHolder.exact(bitmap);
        }
        return null;
      case "isnotnull":
        if (metric instanceof SecondaryIndex.SupportNull) {
          ImmutableBitmap bitmap = ((SecondaryIndex.SupportNull) metric).getNulls(context.baseBitmap);
          if (!withNot) {
            bitmap = DimFilters.complement(context.factory, bitmap, context.numRows());
          }
          return BitmapHolder.exact(bitmap);
        }
        return null;
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
        constants.add(type.cast(((Expression.ConstExpression) expr).get()));
      }
    }
    return constants;
  }

  public static Filter convertToCNF(Filter current)
  {
    return Expressions.convertToCNF(current, new Filter.Factory());
  }

  public static boolean isColumnWithoutBitmap(BitmapIndexSelector selector, String dimension)
  {
    ColumnCapabilities capabilities = selector.getCapabilities(dimension);
    return capabilities != null && !capabilities.hasBitmapIndexes();
  }

  public static IntPredicate toMatcher(ImmutableBitmap bitmapIndex, boolean descending)
  {
    final IntIterator iterator = newIterator(bitmapIndex, descending);
    if (!iterator.hasNext()) {
      return IntPredicate.FALSE;
    }
    if (!descending) {
      return new IntPredicate()
      {
        private int peek = iterator.next();

        @Override
        public boolean apply(int value)
        {
          while (peek >= 0) {
            if (peek == value) {
              peek = iterator.hasNext() ? iterator.next() : -1;
              return true;
            } else if (peek > value) {
              return false;
            } else {
              peek = iterator.hasNext() ? iterator.next() : -1;
            }
          }
          return false;
        }
      };
    } else {
      return new IntPredicate()
      {
        private int peek = iterator.next();

        @Override
        public boolean apply(int value)
        {
          while (peek >= 0) {
            if (peek == value) {
              peek = iterator.hasNext() ? iterator.next() : -1;
              return true;
            } else if (peek < value) {
              return false;
            } else {
              peek = iterator.hasNext() ? iterator.next() : -1;
            }
          }
          return false;
        }
      };
    }
  }

  public static IntIterator newIterator(ImmutableBitmap bitmapIndex, boolean descending)
  {
    if (!descending) {
      return bitmapIndex.iterator();
    }
    ImmutableBitmap roaringBitmap = bitmapIndex;
    if (!(bitmapIndex instanceof WrappedImmutableRoaringBitmap)) {
      final BitmapFactory factory = RoaringBitmapSerdeFactory.bitmapFactory;
      final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
      final IntIterator iterator = bitmapIndex.iterator();
      while (iterator.hasNext()) {
        bitmap.add(iterator.next());
      }
      roaringBitmap = factory.makeImmutableBitmap(bitmap);
    }
    return ((WrappedImmutableRoaringBitmap) roaringBitmap).getBitmap().getReverseIntIterator();
  }

  public static final Filter NONE = new Filter()
  {
    @Override
    public BitmapHolder getBitmapIndex(FilterContext context)
    {
      return BitmapHolder.exact(context.bitmapFactory().makeEmptyImmutableBitmap());
    }

    @Override
    public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
    {
      return ValueMatcher.FALSE;
    }
  };

  public static final Filter ALL = new Filter()
  {
    @Override
    public BitmapHolder getBitmapIndex(FilterContext context)
    {
      return BitmapHolder.exact(DimFilters.makeTrue(context.bitmapFactory(), context.numRows()));
    }

    @Override
    public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
    {
      return ValueMatcher.TRUE;
    }
  };

  // mimic SecondaryIndex with bitmap index
  public static SecondaryIndex asSecondaryIndex(final BitmapIndexSelector selector, final String dimension)
  {
    return new SecondaryIndex.WithRangeAndNull<String>()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.STRING;
      }

      @Override
      public ImmutableBitmap getNulls(ImmutableBitmap baseBitmap)
      {
        return selector.getBitmapIndex(dimension, (String) null);
      }

      @Override
      public BitmapHolder filterFor(Range query, FilterContext context, String attachment)
      {
        return toDimFilter(dimension, query).toFilter(TypeResolver.STRING).getBitmapIndex(context);
      }

      @Override
      public int numRows()
      {
        return selector.getNumRows();
      }

      @Override
      public void close() throws IOException
      {
      }
    };
  }

  public static SecondaryIndex asSecondaryIndex(final GenericColumn column)
  {
    return new SecondaryIndex.WithRangeAndNull<Comparable>()
    {
      @Override
      public ImmutableBitmap getNulls(ImmutableBitmap baseBitmap)
      {
        return column.getNulls();
      }

      @Override
      public ValueDesc type()
      {
        return column.getType();
      }

      @Override
      public BitmapHolder filterFor(Range<Comparable> range, FilterContext context, String attachment)
      {
        return BitmapHolder.exact(scanForRange(column, range, context));
      }

      @Override
      public int numRows()
      {
        return column.getNumRows();
      }

      @Override
      public void close() throws IOException
      {
      }
    };
  }

  private static DimFilter toDimFilter(String dimension, Range range)
  {
    if (range.isEmpty()) {
      return DimFilters.NONE;
    } else if (range.hasLowerBound()) {
      if (range.hasUpperBound()) {
        String lowerEndpoint = (String) range.lowerEndpoint();
        String upperEndpoint = (String) range.upperEndpoint();
        if (Objects.equals(lowerEndpoint, upperEndpoint)) {
          return new SelectorDimFilter(dimension, lowerEndpoint, null);
        }
        boolean lowerStrict = range.lowerBoundType() == BoundType.OPEN;
        boolean upperStrict = range.upperBoundType() == BoundType.OPEN;
        return new BoundDimFilter(dimension, lowerEndpoint, upperEndpoint, lowerStrict, upperStrict, null, null);
      } else if (range.lowerBoundType() == BoundType.OPEN) {
        return BoundDimFilter.gt(dimension, range.lowerEndpoint());
      } else {
        return BoundDimFilter.gte(dimension, range.lowerEndpoint());
      }
    } else if (range.upperBoundType() == BoundType.OPEN) {
      return BoundDimFilter.lt(dimension, range.upperEndpoint());
    } else {
      return BoundDimFilter.lte(dimension, range.upperEndpoint());
    }
  }
}
