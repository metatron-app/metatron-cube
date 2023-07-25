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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.cache.SessionCache;
import io.druid.collections.IntList;
import io.druid.common.Cacheable;
import io.druid.common.Scannable;
import io.druid.common.guava.BinaryRef;
import io.druid.common.guava.BufferWindow;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IntPredicate;
import io.druid.common.utils.IOUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
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
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.ordering.Direction;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ExprEvalColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.bitmap.BitSets;
import io.druid.segment.bitmap.ExtendedBitmap;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.SecondaryIndex;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.DictionaryCompareOp;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

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
    return GuavaUtils.transform(dimFilters, input -> input.toFilter(resolver));
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

  public static boolean matchAll(Filter matcher)
  {
    return matcher == null || matcher == Filters.ALL;
  }

  public static boolean matchNone(Filter matcher)
  {
    return matcher == Filters.NONE;
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toValueMatcher(ColumnSelectorFactory factory, String column, Predicate predicate)
  {
    ValueDesc columnType = factory.resolve(column);
    if (columnType == null) {
      return BooleanValueMatcher.of(predicate.apply(null));
    }
    if (columnType.isDimension()) {
      return Filters.toValueMatcher(factory.makeDimensionSelector(DefaultDimensionSpec.of(column)), predicate);
    }
    return Filters.toValueMatcher(toPredicatable(factory, column, columnType), predicate);
  }

  @SuppressWarnings("unchecked")
  private static ObjectColumnSelector toPredicatable(ColumnSelectorFactory factory, String column, ValueDesc type)
  {
    ObjectColumnSelector selector = factory.makeObjectColumnSelector(column);
    if (type.isPrimitive()) {
      return selector;
    }
    if (ValueDesc.isIndexedId(type)) {
      return ColumnSelectors.asValued(selector);
    }
    if (type.isArray()) {
      return ColumnSelectors.asArray(selector, type.unwrapArray());
    }
    // toString, whatsoever
    return ObjectColumnSelector.string(() -> Objects.toString(selector.get(), null));
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toValueMatcher(final ObjectColumnSelector selector, final Predicate predicate)
  {
    final ValueDesc type = selector.type();
    if (type.isArray() || type.isMultiValued()) {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final Object object = selector.get();
          if (object == null) {
            return predicate.apply(object);
          }
          if (object instanceof List) {
            final List list = (List) object;
            for (int i = 0; i < list.size(); i++) {
              if (predicate.apply(list.get(i))) {
                return true;
              }
            }
            return false;
          } else if (object.getClass().isArray()) {
            final int length = Array.getLength(object);
            for (int i = 0; i < length; i++) {
              if (predicate.apply(Array.get(object, i))) {
                return true;
              }
            }
            return false;
          } else {
            return predicate.apply(object);
          }
        }
      };
    }
    return () -> predicate.apply(selector.get());
  }

  private static ValueMatcher toValueMatcher(final DimensionSelector selector, final Predicate predicate)
  {
    final boolean allowNull = predicate.apply(null);
    // Check every value in the dimension, as a String.
    final int cardinality = selector.getValueCardinality();
    if (cardinality < 0) {
      if (selector instanceof DimensionSelector.SingleValued) {
        return () -> predicate.apply(selector.lookupName(selector.getRow().get(0)));
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
    if (selector instanceof DimensionSelector.SingleValued) {
      return () -> valueIds.get(selector.getRow().get(0));
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

  public static DimensionSelector combine(final DimensionSelector selector, final Predicate predicate)
  {
    return combineIx(selector, v -> predicate.apply(selector.lookupName(v)));
  }

  public static DimensionSelector combineIx(final DimensionSelector selector, final IntPredicate predicate)
  {
    return new DimensionSelector.Delegated(selector)
    {
      private final IntList scratch = new IntList();

      @Override
      public IndexedInts getRow()
      {
        final IndexedInts row = delegate.getRow();
        final int size = row.size();
        if (size == 0) {
          return row;
        }
        if (size == 1) {
          final boolean match = predicate.apply(row.get(0));
          return match ? row : IndexedInts.from(-1);
        }
        scratch.clear();
        boolean allMatched = true;
        for (int i = 0; i < size; i++) {
          final int v = row.get(i);
          final boolean match = predicate.apply(v);
          scratch.add(match ? v : -1);
          allMatched &= match;
        }
        return allMatched ? row : IndexedInts.from(scratch.array());
      }
    };
  }

  public static ValueMatcher toValueMatcher(
      final DimensionSelector selector,
      final IntPredicate predicate,
      final boolean allowNull
  )
  {
    if (selector instanceof DimensionSelector.SingleValued) {
      return () -> predicate.apply(selector.getRow().get(0));
    }
    return () -> {
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
    };
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
  public static ImmutableBitmap matchPredicate(String dimension, Predicate<String> predicate, FilterContext context)
  {
    return matchDictionary(dimension, context, d -> predicate);
  }

  public static ImmutableBitmap matchDictionary(
      final String dimension,
      final FilterContext context,
      final DictionaryMatcher matcher
  )
  {
    final BitmapIndexSelector selector = context.indexSelector();

    // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
    final Column column = selector.getColumn(dimension);
    if (column == null) {
      return selector.createBoolean(matcher.matcher(null).apply(null));
    }
    final BitmapFactory factory = selector.getBitmapFactory();
    final BitmapIndex bitmapIndex = column.getBitmapIndex();
    if (bitmapIndex == null) {
      if (column.hasGenericColumn() && Scannable.class.isAssignableFrom(column.getGenericColumnType())) {
        final GenericColumn generic = column.getGenericColumn();
        try {
          final MutableBitmap mutable = factory.makeEmptyMutableBitmap();
          if (matcher instanceof DictionaryMatcher.RawSupport && generic instanceof Scannable.BufferBacked) {
            final BufferWindow window = new BufferWindow();
            final Predicate<BinaryRef> predicate = ((DictionaryMatcher.RawSupport) matcher).rawMatcher(null);
            final Scannable.BufferBacked scannable = (Scannable.BufferBacked) generic;
            scannable.scan(
                context.rowIterator(), (x, b, o, l) -> {if (predicate.apply(window.set(b, o, l))) {mutable.add(x);}}
            );
          } else {
            final Predicate<String> predicate = matcher.matcher(null);
            final Scannable<String> scannable = (Scannable) generic;
            scannable.scan(context.rowIterator(), (ix, v) -> {if (predicate.apply(v)) {mutable.add(ix);}});
          }
          return factory.makeImmutableBitmap(mutable);
        }
        finally {
          IOUtils.closePropagate(generic);
        }
      }
      return null;
    }
    // Apply predicate to all dimension values and union the matching bitmaps
    final DictionaryEncodedColumn encoded = column.getDictionaryEncoded();
    try {
      final Dictionary<String> dictionary = encoded.dictionary();
      final int cardinality = context.dictionaryRange(dimension, dictionary.size());

      final IntIterator iterator;
      if (encoded.hasMultipleValues() || !context.bitmapFiltered() || cardinality < context.targetNumRows()) {
        iterator = context.dictionaryIterator(dimension);
      } else {
        iterator = BitSets.iterator(encoded.collect(context.rowIterator()));
      }
      final IntIterator cursor = matcher.wrap(dictionary, iterator);
      if (cursor != null && !cursor.hasNext()) {
        return factory.makeEmptyImmutableBitmap();
      }
      final IntList matched = new IntList();
      if (matcher instanceof DictionaryMatcher.RawSupport && dictionary instanceof Scannable.BufferBacked) {
        final BufferWindow window = new BufferWindow();
        final Predicate<BinaryRef> predicate = ((DictionaryMatcher.RawSupport) matcher).rawMatcher(dictionary);
        dictionary.scan(cursor, (x, b, o, l) -> {if (predicate.apply(window.set(b, o, l))) {matched.add(x);}});
      } else {
        final Predicate<String> predicate = matcher.matcher(dictionary);
        dictionary.scan(cursor, (ix, v) -> {if (predicate.apply(v)) {matched.add(ix);}});
      }
      final GenericIndexed<ImmutableBitmap> bitmaps = bitmapIndex.getBitmaps();
      return DimFilters.union(factory, matched.transform(x -> bitmaps.get(x)));
    } finally {
      IOUtils.closePropagate(encoded);
    }
  }

  public static Set<String> getDependents(DimFilter filter)
  {
    if (filter == null) {
      return ImmutableSet.of();
    }
    Set<String> handler = Sets.newHashSet();
    filter.addDependent(handler);
    return handler;
  }

  public static FilterContext createContext(
      final BitmapIndexSelector selector,
      final SessionCache cache,
      final String namespace
  )
  {
    if (cache == null || namespace == null) {
      return new FilterContext(selector);
    }
    return new FilterContext(selector)
    {
      @Override
      public BitmapHolder createBitmap(Cacheable filter, Supplier<BitmapHolder> populator)
      {
        return cache.get(namespace, filter, populator);
      }

      @Override
      public void cache(Cacheable filter, BitmapHolder holder)
      {
        cache.cache(namespace, filter, holder);
      }
    };
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
          Predicate predicate = ((BoundDimFilter) filter).toPredicate(selector);
          bitmap = scanForPredicate(column.getGenericColumn(), predicate, context);
        } else if (filter instanceof SelectorDimFilter) {
          String value = ((SelectorDimFilter) filter).getValue();
          bitmap = scanForEqui(column.getGenericColumn(), value, context);
        } else if (filter instanceof InDimFilter) {
          List<String> values = ((InDimFilter) filter).getValues();
          bitmap = scanForEqui(column.getGenericColumn(), values, context);
        }
        if (bitmap != null) {
          return BitmapHolder.exact(bitmap);
        }
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static ImmutableBitmap scanForPredicate(GenericColumn column, Predicate predicate, FilterContext context)
  {
    final BitmapFactory factory = context.factory;
    try {
      if (column instanceof GenericColumn.FloatType) {
        return ((GenericColumn.FloatType) column).collect(factory, context.rowIterator(), f -> predicate.apply(f));
      } else if (column instanceof GenericColumn.DoubleType) {
        return ((GenericColumn.DoubleType) column).collect(factory, context.rowIterator(), d -> predicate.apply(d));
      } else if (column instanceof GenericColumn.LongType) {
        return ((GenericColumn.LongType) column).collect(factory, context.rowIterator(), l -> predicate.apply(l));
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

  private static ImmutableBitmap scanForRange(GenericColumn column, Range range, FilterContext context)
  {
    final BitmapFactory factory = context.factory;
    try {
      final BoundType lowerType = range.hasLowerBound() ? range.lowerBoundType() : null;
      final BoundType upperType = range.hasUpperBound() ? range.upperBoundType() : null;
      if (column instanceof GenericColumn.FloatType) {
        final float lower = range.hasLowerBound() ? (Float) range.lowerEndpoint() : 0;
        final float upper = range.hasUpperBound() ? (Float) range.upperEndpoint() : 0;
        return ((GenericColumn.FloatType) column).collect(
            factory, context.rowIterator(),
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
            factory, context.rowIterator(),
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
            factory, context.rowIterator(),
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
    final BitmapFactory factory = context.factory;
    try {
      if (StringUtils.isNullOrEmpty(value)) {
        return column.getNulls();
      } else if (column instanceof GenericColumn.FloatType) {
        final BigDecimal decimal = Rows.parseDecimal(value);
        final float fv = decimal.floatValue();
        if (decimal.compareTo(BigDecimal.valueOf(fv)) == 0) {
          return ((GenericColumn.FloatType) column).collect(factory, context.rowIterator(), f -> f == fv);
        }
        return context.factory.makeEmptyImmutableBitmap();
      } else if (column instanceof GenericColumn.DoubleType) {
        final BigDecimal decimal = Rows.parseDecimal(value);
        final double dv = decimal.doubleValue();
        if (decimal.compareTo(BigDecimal.valueOf(dv)) == 0) {
          return ((GenericColumn.DoubleType) column).collect(factory, context.rowIterator(), d -> d == dv);
        }
        return context.factory.makeEmptyImmutableBitmap();
      } else if (column instanceof GenericColumn.LongType) {
        final BigDecimal decimal = Rows.parseDecimal(value);
        final long lv = decimal.longValue();
        if (decimal.compareTo(BigDecimal.valueOf(lv)) == 0) {
          return ((GenericColumn.LongType) column).collect(factory, context.rowIterator(), l -> l == lv);
        }
        return context.factory.makeEmptyImmutableBitmap();
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

  private static ImmutableBitmap scanForEqui(GenericColumn column, List<String> values, FilterContext context)
  {
    final BitmapFactory factory = context.factory;

    final boolean containsNull = values.contains("");
    final List<BigDecimal> decimals = ImmutableList.copyOf(
        Iterables.filter(Iterables.transform(values, Rows::tryParseDecimal), Predicates.notNull())
    );
    if (decimals.isEmpty()) {
      return containsNull ? column.getNulls() : factory.makeEmptyImmutableBitmap();
    }
    try {
      ImmutableBitmap nulls = containsNull ? column.getNulls() : factory.makeEmptyImmutableBitmap();
      ImmutableBitmap collected = null;
      if (column instanceof GenericColumn.FloatType) {
        final GenericColumn.FloatType floatType = (GenericColumn.FloatType) column;
        final FloatSet set = new FloatOpenHashSet();
        for (BigDecimal decimal : decimals) {
          final float fv = decimal.floatValue();
          if (decimal.compareTo(BigDecimal.valueOf(fv)) == 0) {
            set.add(fv);
          }
        }
        if (!set.isEmpty()) {
          collected = floatType.collect(factory, context.rowIterator(), f -> set.contains(f));
        } else {
          collected = factory.makeEmptyImmutableBitmap();
        }
      } else if (column instanceof GenericColumn.DoubleType) {
        final GenericColumn.DoubleType doubleType = (GenericColumn.DoubleType) column;
        final DoubleSet set = new DoubleOpenHashSet();
        for (BigDecimal decimal : decimals) {
          final double dv = decimal.doubleValue();
          if (decimal.compareTo(BigDecimal.valueOf(dv)) == 0) {
            set.add(dv);
          }
        }
        if (!set.isEmpty()) {
          collected = doubleType.collect(factory, context.rowIterator(), d -> set.contains(d));
        } else {
          collected = factory.makeEmptyImmutableBitmap();
        }
      } else if (column instanceof GenericColumn.LongType) {
        final GenericColumn.LongType longType = (GenericColumn.LongType) column;
        final LongSet set = new LongOpenHashSet();
        for (BigDecimal decimal : decimals) {
          final long lv = decimal.longValue();
          if (decimal.compareTo(BigDecimal.valueOf(lv)) == 0) {
            set.add(lv);
          }
        }
        if (!set.isEmpty()) {
          collected = longType.collect(factory, context.rowIterator(), l -> set.contains(l));
        } else {
          collected = factory.makeEmptyImmutableBitmap();
        }
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

  public static Filter ofExpr(final DimFilter source, final Expr expr)
  {
    return new Filter()
    {
      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        return toExprBitmap(expr, context, false);
      }

      private BitmapHolder toExprBitmap(Expression expr, FilterContext context, boolean withNot)
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
        }
        List<String> required = Parser.findRequiredBindings((Expr) expr);
        if (required.size() == 1) {
          return handleSoleDimension((Expr) expr, required.get(0), withNot, context);
        }
        if (required.size() == 2) {
          return handleBiDimension((Expr) expr, withNot, context);
        }
        return null;
      }

      @Nullable
      private BitmapHolder handleSoleDimension(Expr expr, String columnName, boolean withNot, FilterContext context)
      {
        BitmapIndexSelector selector = context.selector;
        Column column = selector.getColumn(columnName);
        if (column == null) {
          return null;  // todo
        }
        ColumnCapabilities capabilities = column.getCapabilities();
        if (Evals.isLeafFunction((Expr) expr, columnName) && isRangeCompatible(((FuncExpression) expr).op())) {
          FuncExpression funcExpr = (FuncExpression) expr;
          if (capabilities.hasBitmapIndexes()) {
            SecondaryIndex.WithRange index = asSecondaryIndex(selector, columnName);
            BitmapHolder holder = leafToRanges(columnName, funcExpr, context, index, withNot);
            if (holder != null) {
              return holder;
            }
          }
          SecondaryIndex index = getWhatever(selector, columnName, SecondaryIndex.class);
          if (index instanceof SecondaryIndex.WithRange) {
            long start = System.currentTimeMillis();
            BitmapHolder holder = leafToRanges(columnName, funcExpr, context, (SecondaryIndex.WithRange) index, withNot);
            if (holder != null) {
              if (logger.isDebugEnabled()) {
                long elapsed = System.currentTimeMillis() - start;
                logger.debug(
                    "%s%s : %,d / %,d (%,d msec)", withNot ? "!" : "", expr, holder.size(), context.numRows(), elapsed
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
        final List<ImmutableBitmap> bitmaps = Lists.newArrayList();
        final Expr.Bindable binding = Parser.bindable(columnName);

        final BitmapIndex bitmapIndex = selector.getBitmapIndex(columnName);
        if (bitmapIndex != null) {
          Dictionary<String> dictionary = bitmapIndex.getDictionary().dedicated();
          IntList ids = new IntList();
          dictionary.scan(context.dictionaryIterator(columnName), (ix, v) -> {
            if (expr.eval(binding.bind(columnName, v)).asBoolean()) {ids.add(ix);}
          });
          if (context.isRoot(source)) {
            context.dictionaryRef(columnName, RoaringBitmapFactory.from(ids.array()));
          }
          ids.stream().mapToObj(ix -> bitmapIndex.getBitmap(ix)).forEach(bitmaps::add);
        } else if (capabilities.getType() == ValueType.BOOLEAN) {
          for (Boolean bool : new Boolean[]{null, true, false}) {
            if (expr.eval(binding.bind(columnName, bool)).asBoolean()) {
              bitmaps.add(selector.getBitmapIndex(columnName, bool));
            }
          }
        } else {
          return null;
        }
        ImmutableBitmap bitmap = DimFilters.union(context.factory, bitmaps);
        if (withNot) {
          bitmap = DimFilters.complement(context.factory, bitmap, context.numRows());
        }
        return BitmapHolder.exact(bitmap);
      }

      private BitmapHolder handleBiDimension(Expr expr, boolean withNot, FilterContext context)
      {
        if (context.targetNumRows() < (context.numRows() >> 3)) {
          return null;
        }
        BitmapIndexSelector selector = context.selector;
        Parser.SimpleBinaryOp binaryOp =
            Parser.isBinaryRangeOpWith(expr, c -> selector.getCapabilities(c) != null &&
                                                  selector.getCapabilities(c).isDictionaryEncoded() &&
                                                  !selector.getCapabilities(c).hasMultipleValues());
        if (binaryOp == null || Dictionary.compareOp(binaryOp.op) == null) {
          return null;
        }
        DictionaryCompareOp op = Dictionary.compareOp(binaryOp.op);

        Column column1 = selector.getColumn(binaryOp.left);
        Column column2 = selector.getColumn(binaryOp.right);

        BitmapIndex[] bitmaps = new BitmapIndex[]{column1.getBitmapIndex(), column2.getBitmapIndex()};
        DictionaryEncodedColumn[] encoded = new DictionaryEncodedColumn[]{
            column1.getDictionaryEncoded(), column2.getDictionaryEncoded()
        };

        if (bitmaps[0].getCardinality() > bitmaps[1].getCardinality()) {
          op = op.flip();
          GuavaUtils.swap(encoded, 0, 1);
          GuavaUtils.swap(bitmaps, 0, 1);
        }

        final int numRows = context.numRows();
        final BufferWindow window = new BufferWindow();
        try {
          final Stream<BinaryRef> stream = bitmaps[0].getDictionary().apply(
              null, (ix, buffer, offset, length) -> window.set(buffer, offset, length)
          );
          final int ratio = bitmaps[0].getCardinality() / bitmaps[1].getCardinality();
          final int[] indices = bitmaps[1].getDictionary().indexOfRaw(stream, ratio >= 8).map(op::ix).toArray();

          ImmutableBitmap bitmap;
          if (context.factory instanceof RoaringBitmapFactory) {
            final IntIterator it0 = encoded[0].getSingleValueRows();
            final IntIterator it1 = encoded[1].getSingleValueRows();
            final long[] words = BitSets.makeWords(numRows);
            for (int i = 0; i < words.length; i++) {
              final int offset = i << BitSets.ADDRESS_BITS_PER_WORD;
              final int limit = Math.min(numRows - offset, BitSets.BITS_PER_WORD);
              long v = 0;
              for (int x = 0; x < limit; x++) {
                if (op.compare(indices[it0.next()], it1.next())) {
                  v += 1L << x;
                }
              }
              words[i] = v;
            }
            final BitSet bitSet = BitSet.valueOf(words);
            if (withNot) {
              bitSet.flip(0, numRows);
            }
            bitmap = RoaringBitmapFactory.from(bitSet);
          } else {
            final IntIterator it0 = encoded[0].getSingleValueRows();
            final IntIterator it1 = encoded[1].getSingleValueRows();
            final MutableBitmap mutable = context.factory.makeEmptyMutableBitmap();
            for (int i = 0; i < numRows; i++) {
              if (op.compare(indices[it0.next()], it1.next())) {
                mutable.add(i);
              }
            }
            bitmap = context.factory.makeImmutableBitmap(mutable);
            if (withNot) {
              bitmap = DimFilters.complement(context.factory, bitmap, numRows);
            }
          }
          return BitmapHolder.exact(bitmap);
        }
        finally {
          IOUtils.closeQuietly(encoded[0]);
          IOUtils.closeQuietly(encoded[1]);
        }
      }

      @Override
      public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
      {
        final ExprEvalColumnSelector selector = factory.makeMathExpressionSelector(expr);
        return () -> selector.get().asBoolean();
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <T extends SecondaryIndex> T getWhatever(BitmapIndexSelector bitmaps, String column, Class<T> type)
  {
    SecondaryIndex bitmap = bitmaps.getBitSlicedBitmap(column);
    if (!type.isInstance(bitmap)) {
      bitmap = bitmaps.getExternalIndex(column, type);
    }
    if (!type.isInstance(bitmap)) {
      bitmap = bitmaps.getMetricBitmap(column);
    }
    return type.isInstance(bitmap) ? (T) bitmap : null;
  }

  private static Set<String> RANGE_COMPATIBLE = ImmutableSet.of("between", "in", "isnull", "isnotnull");

  private static boolean isRangeCompatible(String op)
  {
    return Expressions.isCompare(op) || RANGE_COMPATIBLE.contains(op.toLowerCase());
  }

  // constants need to be calculated apriori
  @SuppressWarnings("unchecked")
  private static BitmapHolder leafToRanges(
      String column,
      FuncExpression expression,
      FilterContext context,
      SecondaryIndex.WithRange metric,
      boolean withNot
  )
  {
    final ValueType type = ValueDesc.assertPrimitive(metric.type()).type();
    if (Expressions.isCompare(expression.op())) {
      final Comparable constant = getOnlyConstant(expression.getChildren(), type);
      if (constant != null) {
        return metric.compare(expression.op(), withNot, column, constant, context);
      }
    }

    switch (expression.op().toLowerCase()) {
      case "between":
        List<Comparable> range = getConstants(expression.getChildren(), type);
        if (range == null || range.size() != 2) {
          return null;
        }
        Comparable value1 = range.get(0);
        Comparable value2 = range.get(1);
        if (value1 == null || value2 == null) {
          return null;
        }
        if (value1.compareTo(value2) > 0) {
          Comparable x = value1;
          value1 = value2;
          value2 = x;
        }
        return metric.between(withNot, column, value1, value2, context);
      case "in":
        if (withNot) {
          return null;  // hard to be expressed with bitmap
        }
        List<BitmapHolder> holders = Lists.newArrayList();
        List<Comparable> values = getConstants(expression.getChildren(), type);
        if (values == null) {
          return null;
        }
        return metric.in(column, values, context);
      case "isnull":
        if (metric instanceof SecondaryIndex.SupportNull) {
          ImmutableBitmap bitmap = ((SecondaryIndex.SupportNull) metric).getNulls();
          if (withNot) {
            bitmap = DimFilters.complement(context.factory, bitmap, context.numRows());
          }
          return BitmapHolder.exact(bitmap);
        }
        return null;
      case "isnotnull":
        if (metric instanceof SecondaryIndex.SupportNull) {
          ImmutableBitmap bitmap = ((SecondaryIndex.SupportNull) metric).getNulls();
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
        constant = (Comparable) type.cast(((Expression.ConstExpression) expr).get());
      }
    }
    return constant;
  }

  private static List<Comparable> getConstants(List<Expression> children, ValueType type)
  {
    List<Comparable> constants = Lists.newArrayList();
    for (Expression expr : children) {
      if (expr instanceof Expression.ConstExpression) {
        constants.add((Comparable) type.cast(((Expression.ConstExpression) expr).get()));
      } else {
        return null;
      }
    }
    return constants;
  }

  private static final Filter.Factory FACTORY = new Filter.Factory();

  public static Filter convertToCNF(Filter current)
  {
    return Expressions.convertToCNF(current, FACTORY);
  }

  public static IntPredicate toMatcher(ImmutableBitmap bitmapIndex, Direction direction)
  {
    final IntIterator iterator = newIterator(bitmapIndex, direction, -1);
    if (!iterator.hasNext()) {
      return IntPredicate.FALSE;
    }
    if (direction == Direction.ASCENDING) {
      return new IntPredicate()
      {
        private int peek = iterator.next();

        @Override
        public boolean apply(int value)
        {
          while (peek >= 0) {
            if (peek == value) {
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

  // offset : best effort
  public static IntIterator newIterator(ImmutableBitmap bitmapIndex, Direction direction, int offset)
  {
    if (direction == Direction.ASCENDING) {
      if (offset > 0 && bitmapIndex instanceof ExtendedBitmap) {
        return ((ExtendedBitmap) bitmapIndex).iterator(offset);
      }
      return bitmapIndex.iterator();
    }
    // todo: offset
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
    public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
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
    public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
    {
      return ValueMatcher.TRUE;
    }
  };

  // mimic SecondaryIndex with bitmap index
  public static SecondaryIndex.WithRange asSecondaryIndex(final BitmapIndexSelector selector, final String dimension)
  {
    return new SecondaryIndex.WithRangeAndNull<String>()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.STRING;
      }

      @Override
      public ImmutableBitmap getNulls()
      {
        return selector.getBitmapIndex(dimension, (String) null);
      }

      @Override
      public BitmapHolder filterFor(Range query, FilterContext context, String attachment)
      {
        return toDimFilter(dimension, query).toFilter(TypeResolver.STRING).getBitmapIndex(context);
      }

      @Override
      public void close() throws IOException
      {
      }
    };
  }

  public static SecondaryIndex.WithRange asSecondaryIndex(final GenericColumn column)
  {
    return new SecondaryIndex.WithRangeAndNull<Comparable>()
    {
      @Override
      public ImmutableBitmap getNulls()
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
