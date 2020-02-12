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
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.IntPredicate;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
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
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.select.Schema;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ExprEvalColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.bitmap.BitSetBitmapFactory;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.column.SecondaryIndex;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.lucene.Lucenes;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
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
        return DimFilters.makeTrue(selector.getBitmapFactory(), selector.getNumRows());
      } else {
        return DimFilters.makeFalse(selector.getBitmapFactory());
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

  public static Set<String> getDependents(DimFilter filter)
  {
    Set<String> handler = Sets.newHashSet();
    if (filter != null) {
      filter.addDependent(handler);
    }
    return handler;
  }

  public static class BitmapHolder extends Pair<Boolean, ImmutableBitmap>
  {
    public static BitmapHolder of(boolean exact, ImmutableBitmap rhs)
    {
      return new BitmapHolder(exact, rhs);
    }

    public static BitmapHolder exact(ImmutableBitmap rhs)
    {
      return new BitmapHolder(true, rhs);
    }

    public static BitmapHolder notExact(ImmutableBitmap rhs)
    {
      return new BitmapHolder(false, rhs);
    }

    private BitmapHolder(boolean lhs, ImmutableBitmap rhs)
    {
      super(lhs, rhs);
    }

    public boolean exact()
    {
      return lhs;
    }

    public ImmutableBitmap bitmap()
    {
      return rhs;
    }
  }

  private static final BitmapFactory[] BITMAP_FACTORIES = new BitmapFactory[]{
      new BitSetBitmapFactory(), new ConciseBitmapFactory(), new RoaringBitmapFactory()
  };

  public static FilterContext getFilterContext(
      final BitmapIndexSelector selector,
      final Cache cache,
      final String segmentId
  )
  {
    final byte bitmapCode = codeOfFactory(selector.getBitmapFactory());
    if (cache == null || segmentId == null || bitmapCode >= BITMAP_FACTORIES.length) {
      return new FilterContext(selector);
    }
    final byte[] namespace = io.druid.java.util.common.StringUtils.toUtf8(segmentId);
    return new FilterContext(selector)
    {
      @Override
      public BitmapHolder createBitmap(DimFilter filter)
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
        final BitmapHolder holder = super.createBitmap(filter);
        if (holder != null) {
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

  public static class FilterContext implements Closeable
  {
    protected final BitmapIndexSelector selector;
    protected final BitmapFactory factory;
    protected ImmutableBitmap baseBitmap;

    public FilterContext(BitmapIndexSelector selector)
    {
      this.selector = selector;
      this.factory = selector.getBitmapFactory();
    }

    public BitmapHolder createBitmap(DimFilter filter)
    {
      long start = System.currentTimeMillis();
      BitmapHolder holder = leafToBitmap(filter, this);
      if (holder != null && logger.isDebugEnabled()) {
        long elapsed = System.currentTimeMillis() - start;
        logger.debug("%s : %,d / %,d (%,d msec)", filter, holder.bitmap().size(), getNumRows(), elapsed);
      }
      return holder;
    }

    public BitmapFactory bitmapFactory()
    {
      return factory;
    }

    public ImmutableBitmap intersection(List<ImmutableBitmap> bitmaps)
    {
      return factory.intersection(bitmaps);
    }

    public ImmutableBitmap union(List<ImmutableBitmap> bitmaps)
    {
      return factory.union(bitmaps);
    }

    public ImmutableBitmap not(ImmutableBitmap bitmaps)
    {
      return factory.complement(bitmaps, getNumRows());
    }

    public void setBaseBitmap(ImmutableBitmap baseBitmap)
    {
      this.baseBitmap = baseBitmap;
    }

    public void andBaseBitmap(ImmutableBitmap newBaseBitmap)
    {
      baseBitmap = baseBitmap == null ? newBaseBitmap : factory.intersection(Arrays.asList(baseBitmap, newBaseBitmap));
    }

    public int getNumRows()
    {
      return selector.getNumRows();
    }

    @Override
    public void close()
    {
      selector.close();
    }
  }

  public static BitmapHolder toBitmapHolder(DimFilter dimFilter, FilterContext context)
  {
    long start = System.currentTimeMillis();
    BitmapHolder baseBitmap = toBitmapHolderRecurse(dimFilter, context);
    if (baseBitmap != null && logger.isDebugEnabled()) {
      long elapsed = System.currentTimeMillis() - start;
      logger.debug("%s : %,d / %,d (%,d msec)", dimFilter, baseBitmap.rhs.size(), context.getNumRows(), elapsed);
    }
    return baseBitmap;
  }

  private static BitmapHolder toBitmapHolderRecurse(DimFilter dimFilter, FilterContext context)
  {
    if (dimFilter instanceof RelationExpression) {
      boolean exact = true;
      ImmutableBitmap prev = context.baseBitmap;
      RelationExpression relation = (RelationExpression) dimFilter;
      boolean andExpression = relation instanceof AndExpression;
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      for (Expression child : relation.getChildren()) {
        BitmapHolder holder = toBitmapHolderRecurse((DimFilter) child, context);
        if (holder == null && !andExpression) {
          return null;
        }
        if (holder != null) {
          exact &= holder.exact();
          bitmaps.add(holder.bitmap());
          if (andExpression) {
            context.andBaseBitmap(holder.bitmap());
          }
        } else {
          exact = false;
        }
      }
      if (andExpression) {
        context.baseBitmap = prev;
      }
      if (!bitmaps.isEmpty()) {
        // concise returns 1,040,187,360. roaring returns 0. makes wrong result anyway
        if (dimFilter instanceof AndDimFilter) {
          return new BitmapHolder(exact, context.intersection(bitmaps));
        } else if (dimFilter instanceof OrDimFilter) {
          return new BitmapHolder(exact, context.union(bitmaps));
        } else {
          return new BitmapHolder(exact, context.not(Iterables.getOnlyElement(bitmaps)));
        }
      }
    } else {
      return context.createBitmap(dimFilter);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static BitmapHolder leafToBitmap(DimFilter filter, FilterContext context)
  {
    Preconditions.checkArgument(!(filter instanceof RelationExpression));
    if (filter instanceof DimFilter.ValueOnly) {
      return null;
    }
    final BitmapIndexSelector selector = context.selector;
    final Schema schema = selector.getSchema(true);
    if (filter instanceof MathExprFilter) {
      Expr expr = Parser.parse(((MathExprFilter) filter).getExpression(), schema);
      Expr cnf = Expressions.convertToCNF(expr, Parser.EXPR_FACTORY);
      ImmutableBitmap bitmap = toExprBitmap(cnf, context, false);
      return bitmap == null ? null : BitmapHolder.exact(bitmap);
    } else if (filter instanceof DimFilter.LuceneFilter) {
      // throws exception.. cannot support value matcher
      return BitmapHolder.exact(filter.toFilter(schema).getBitmapIndex(selector, context.baseBitmap));
    }
    Set<String> dependents = Filters.getDependents(filter);
    if (dependents.size() != 1) {
      return null;
    }
    String column = Iterables.getOnlyElement(dependents);
    ColumnCapabilities capabilities = selector.getCapabilities(column);
    if (capabilities == null) {
      return null;
    }
    if (capabilities.hasBitmapIndexes()) {
      return BitmapHolder.exact(filter.toFilter(schema).getBitmapIndex(selector, context.baseBitmap));
    }
    if (capabilities.getType() == ValueType.BOOLEAN && filter instanceof DimFilter.BooleanColumnSupport) {
      ImmutableBitmap bitmap = ((DimFilter.BooleanColumnSupport) filter).toBooleanFilter(schema, selector);
      if (bitmap != null) {
        return BitmapHolder.exact(bitmap);
      }
    }
    if (filter instanceof DimFilter.RangeFilter) {
      SecondaryIndex.WithRange index = getWhatever(selector, column, SecondaryIndex.WithRange.class);
      if (index == null) {
        return null;
      }
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      for (Range range : ((DimFilter.RangeFilter) filter).toRanges(asTypeResolver(selector))) {
        bitmaps.add(index.filterFor(range, context.baseBitmap));
      }
      ImmutableBitmap bitmap = context.union(bitmaps);
      return index.isExact() ? BitmapHolder.exact(bitmap) : BitmapHolder.notExact(bitmap);
    }
    return null;
  }

  private static ImmutableBitmap toExprBitmap(Expression expr, FilterContext context, boolean withNot)
  {
    if (expr instanceof AndExpression) {
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      for (Expression child : ((AndExpression) expr).getChildren()) {
        ImmutableBitmap extracted = toExprBitmap(child, context, withNot);
        if (extracted != null) {
          bitmaps.add(extracted);
        }
      }
      return bitmaps.isEmpty() ? null : context.intersection(bitmaps);
    } else if (expr instanceof OrExpression) {
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      for (Expression child : ((OrExpression) expr).getChildren()) {
        ImmutableBitmap extracted = toExprBitmap(child, context, withNot);
        if (extracted == null) {
          return null;
        }
        bitmaps.add(extracted);
      }
      return bitmaps.isEmpty() ? null : context.union(bitmaps);
    } else if (expr instanceof NotExpression) {
      return toExprBitmap(((NotExpression) expr).getChild(), context, !withNot);
    } else {
      List<String> required = Parser.findRequiredBindings((Expr) expr);
      if (required.size() != 1) {
        return null;
      }
      String columnName = required.get(0);
      BitmapIndexSelector selector = context.selector;
      ColumnCapabilities capabilities = selector.getCapabilities(columnName);
      if (capabilities == null) {
        return null;
      }
      if (Evals.isLeafFunction((Expr) expr, columnName)) {
        FuncExpression funcExpr = (FuncExpression) expr;
        if (capabilities.hasBitmapIndexes()) {
          SecondaryIndex index = asSecondaryIndex(selector, columnName);
          ImmutableBitmap bitmap = leafToRanges(columnName, funcExpr, context, index, withNot);
          if (bitmap != null) {
            return bitmap;
          }
        }
        // can be null for complex column
        GenericColumn column = selector.getColumn(columnName).getGenericColumn();
        if (column != null) {
          ImmutableBitmap bitmap = leafToRanges(columnName, funcExpr, context, asSecondaryIndex(column), withNot);
          if (bitmap != null) {
            return bitmap;
          }
        }
        SecondaryIndex index = getWhatever(selector, columnName, SecondaryIndex.class);
        if (index != null) {
          long start = System.currentTimeMillis();
          ImmutableBitmap bitmap = leafToRanges(columnName, funcExpr, context, index, withNot);
          if (bitmap != null) {
            if (logger.isDebugEnabled()) {
              long elapsed = System.currentTimeMillis() - start;
              logger.debug(
                  "%s%s : %,d / %,d (%,d msec)", withNot ? "!" : "", expr, bitmap.size(), index.numRows(), elapsed
              );
            }
            return bitmap;
          }
        }
      }
      if (capabilities.hasBitmapIndexes() || capabilities.getType() == ValueType.BOOLEAN) {
        // traverse all possible values
        ImmutableBitmap bitmap = ofExpr((Expr) expr).getBitmapIndex(selector, context.baseBitmap);
        if (bitmap != null && withNot) {
          bitmap = context.not(bitmap);
        }
        return bitmap;
      }
      return null;
    }
  }

  public static Filter ofExpr(final Expr expr)
  {
    return new Filter()
    {
      @Override
      public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
      {
        String dimension = Iterables.getOnlyElement(Parser.findRequiredBindings(expr));
        BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);

        BitmapFactory factory = selector.getBitmapFactory();

        final int cardinality = bitmapIndex.getCardinality();
        final DSuppliers.HandOver<String> handOver = new DSuppliers.HandOver<>();
        final Expr.NumericBinding binding = Parser.withSuppliers(ImmutableMap.<String, Supplier>of(dimension, handOver));

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
      public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, ImmutableBitmap baseBitmap)
      {
        final String dimension = Iterables.getOnlyElement(Parser.findRequiredBindings(expr));

        final DSuppliers.HandOver<Object> handOver = new DSuppliers.HandOver<>();
        final Expr.NumericBinding binding = Parser.withSuppliers(ImmutableMap.<String, Supplier>of(dimension, handOver));

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
        } else {
          for (Boolean bool : new Boolean[]{null, true, false}) {
            handOver.set(bool);
            if (expr.eval(binding).asBoolean()) {
              bitmaps.add(selector.getBitmapIndex(dimension, bool));
            }
          }
        }
        if (!bitmaps.isEmpty()) {
          return selector.getBitmapFactory().union(bitmaps);
        }
        return selector.getBitmapFactory().makeEmptyImmutableBitmap();
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
      {
        final ExprEvalColumnSelector selector = columnSelectorFactory.makeMathExpressionSelector(expr);
        return new ValueMatcher()
        {
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
  private static ImmutableBitmap leafToRanges(
      String column,
      FuncExpression expression,
      FilterContext context,
      SecondaryIndex metric,
      boolean withNot
  )
  {
    final ValueType type = ValueDesc.assertPrimitive(metric.type()).type();

    final BitmapFactory factory = context.factory;
    final ImmutableBitmap baseBitmap = context.baseBitmap;
    if (Expressions.isCompare(expression.op())) {
      final Comparable constant = getOnlyConstant(expression.getChildren(), type);
      if (constant == null) {
        return null;
      }
      switch (expression.op()) {
        case "<":
          return metric instanceof SecondaryIndex.WithRange ? metric.filterFor(
              withNot ? Range.atLeast(constant) : Range.lessThan(constant), baseBitmap) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.atLeast(column, constant) : Lucenes.lessThan(column, constant), baseBitmap
                 ) :
                 null;
        case ">":
          return metric instanceof SecondaryIndex.WithRange ? metric.filterFor(
              withNot ? Range.atMost(constant) : Range.greaterThan(constant), baseBitmap) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.atMost(column, constant) : Lucenes.greaterThan(column, constant), baseBitmap
                 ) :
                 null;
        case "<=":
          return metric instanceof SecondaryIndex.WithRange ? metric.filterFor(
              withNot ? Range.greaterThan(constant) : Range.atMost(constant), baseBitmap) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.greaterThan(column, constant) : Lucenes.atMost(column, constant), baseBitmap
                 ) :
                 null;
        case ">=":
          return metric instanceof SecondaryIndex.WithRange ? metric.filterFor(
              withNot ? Range.lessThan(constant) : Range.atLeast(constant), baseBitmap) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.lessThan(column, constant) : Lucenes.atLeast(column, constant), baseBitmap
                 ) :
                 null;
        case "==":
          if (withNot) {
            return factory.union(
                Arrays.asList(
                    metric instanceof SecondaryIndex.WithRange ?
                    metric.filterFor(Range.lessThan(constant), baseBitmap) :
                    metric instanceof LuceneIndex ?
                    metric.filterFor(Lucenes.lessThan(column, constant), baseBitmap) :
                    null,
                    metric instanceof SecondaryIndex.WithRange ?
                    metric.filterFor(Range.greaterThan(constant), baseBitmap) :
                    metric instanceof LuceneIndex ?
                    metric.filterFor(Lucenes.greaterThan(column, constant), baseBitmap) :
                    null
                )
            );
          }
          return metric instanceof SecondaryIndex.WithRange ?
                 metric.filterFor(Range.closed(constant, constant), baseBitmap) :
                 metric instanceof LuceneIndex ?
                 metric.filterFor(Lucenes.point(column, constant), baseBitmap) :
                 null;
      }
    }

    switch (expression.op()) {
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
              Arrays.asList(
                  metric instanceof SecondaryIndex.WithRange ?
                  metric.filterFor(Range.lessThan(value1), baseBitmap) :
                  metric instanceof LuceneIndex ?
                  metric.filterFor(Lucenes.lessThan(column, value1), baseBitmap) :
                  null,
                  metric instanceof SecondaryIndex.WithRange ?
                  metric.filterFor(Range.greaterThan(value2), baseBitmap) :
                  metric instanceof LuceneIndex ? metric.filterFor(Lucenes.greaterThan(column, value2), baseBitmap) :
                  null
              )
          );
        }
        return metric instanceof SecondaryIndex.WithRange ?
               metric.filterFor(Range.closed(value1, value2), baseBitmap) :
               metric instanceof LuceneIndex ?
               metric.filterFor(Lucenes.closed(column, value1, value2), baseBitmap) :
               null;
      case "in":
        if (withNot) {
          return null;  // hard to be expressed with bitmap
        }
        List<ImmutableBitmap> bitmaps = Lists.newArrayList();
        for (Comparable value : getConstants(expression.getChildren(), type)) {
          bitmaps.add(
              metric instanceof SecondaryIndex.WithRange ?
              metric.filterFor(Range.closed(value, value), baseBitmap) :
              metric instanceof LuceneIndex ?
              metric.filterFor(Lucenes.point(column, value), baseBitmap) :
              null
          );
        }
        return bitmaps.isEmpty() ? null : factory.union(bitmaps);
      case "isNull":
        if (metric instanceof SecondaryIndex.SupportNull) {
          ImmutableBitmap bitmap = ((SecondaryIndex.SupportNull) metric).getNulls(baseBitmap);
          if (withNot) {
            bitmap = context.not(bitmap);
          }
          return bitmap;
        }
        return null;
      case "isNotNull":
        if (metric instanceof SecondaryIndex.SupportNull) {
          ImmutableBitmap bitmap = ((SecondaryIndex.SupportNull) metric).getNulls(baseBitmap);
          if (!withNot) {
            bitmap = context.not(bitmap);
          }
          return bitmap;
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

  public static boolean hasBitmapOrNull(BitmapIndexSelector selector, String dimension)
  {
    ColumnCapabilities capabilities = selector.getCapabilities(dimension);
    return capabilities == null || capabilities.hasBitmapIndexes();
  }

  public static IntPredicate toMatcher(ImmutableBitmap bitmapIndex, boolean descending)
  {
    final IntIterator iterator = newIterator(bitmapIndex, descending);
    if (!iterator.hasNext()) {
      return new IntPredicate()
      {
        @Override
        public boolean apply(int value)
        {
          return false;
        }
      };
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

  public static TypeResolver asTypeResolver(final BitmapIndexSelector selector)
  {
    return new TypeResolver()
    {
      @Override
      public ValueDesc resolve(String column)
      {
        final ColumnCapabilities capabilities = selector.getCapabilities(column);
        return capabilities == null ? null : ValueDesc.of(capabilities.getType());
      }
    };
  }

  public static final Filter NONE = new Filter()
  {
    @Override
    public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
    {
      return selector.getBitmapFactory().makeEmptyImmutableBitmap();
    }

    @Override
    public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, ImmutableBitmap baseBitmap)
    {
      return selector.getBitmapFactory().makeEmptyImmutableBitmap();
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
    public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
    {
      return DimFilters.makeTrue(selector.getBitmapFactory(), selector.getNumRows());
    }

    @Override
    public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, ImmutableBitmap baseBitmap)
    {
      return baseBitmap != null ? baseBitmap : DimFilters.makeTrue(selector.getBitmapFactory(), selector.getNumRows());
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
      public ImmutableBitmap filterFor(Range query, ImmutableBitmap baseBitmap)
      {
        return toDimFilter(dimension, query).toFilter(TypeResolver.STRING).getBitmapIndex(selector, baseBitmap);
      }

      @Override
      public boolean isExact()
      {
        return true;
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
    return new SecondaryIndex.SupportNull()
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
      public ImmutableBitmap filterFor(Object query, ImmutableBitmap baseBitmap)
      {
        return null;
      }

      @Override
      public boolean isExact()
      {
        return true;
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
